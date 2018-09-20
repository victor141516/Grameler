from __future__ import with_statement

import datetime
import errno
from functools import wraps
from io import BytesIO
import logging
import os
import requests
import sys

from database import db, File, TelegramDocument
import file_utils
from fuse import FUSE, FuseOSError, Operations
import telebot
import tempfile

log = logging.getLogger(__name__)

def logged(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            ret = f(*args, **kwargs)
            log.info('-------------------------------------\n%s(%s) = %s', f.__name__, ','.join([str(item) for item in args[1:]]), str(ret))
            return ret
        except Exception as e:
            log.error('-------------------------------------\n%s(%s)', f.__name__, ','.join([str(item) for item in args[1:]]))
            log.error(e)
            raise e
    return wrapped

class Grameler(Operations):
    def __init__(self, tg_token, chat_id):
        self._uid = os.getuid()
        self._gid = os.getgid()

        File.get_or_create(
            id=1,
            name='$',
            telegram_file_id=None,
            parent_file_id=None,
            size=512,
            user_owner=self._uid,
            group_owner=self._gid,
            sym_link=None,
            is_directory=1,
            mode=16895
        )
        self.tg_token = tg_token
        self.tgbot = telebot.TeleBot(tg_token)
        self.chat_id = chat_id
        self._tgchunk_size = 20*1024*1024

    # Helpers
    # =======

    @logged
    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path


    @logged
    def _upload_file(self, doc):
        doc.seek(0)
        return self.tgbot.send_document(self.chat_id, doc).document.file_id

    @logged
    def _get_file(self, file_id):
        file_info = self.tgbot.get_file(file_id)
        return requests.get('https://api.telegram.org/file/bot{0}/{1}'.format(self.tg_token, file_info.file_path), stream=True).raw

    @logged
    def _get_path_ids(self, path):
        names = list(filter(None, path.split('/')))
        parent_id = 1
        ids = [1]
        for name in names:
            f = File.get_or_none(File.name == name, File.parent_file == parent_id)
            if f is None:
                raise FuseOSError(errno.ENOENT)
            ids.append(f.id)
            parent_id = f.id

        return ids

    # Filesystem methodsc
    # ==================

    @logged
    def access(self, path, mode):
        self._get_path_ids(path)

    @logged
    def chmod(self, path, mode):
        file_id = self._get_path_ids(path)[-1]
        File.update(mode=mode).where(File.id == file_id).execute()

    @logged
    def chown(self, path, uid, gid):
        file_id = self._get_path_ids(path)[-1]
        File.update(
            user_owner=uid,
            group_owner=gid
        ).where(File.id == file_id).execute()

    # @logged
    def getattr(self, path, fh=None):
        file_id = self._get_path_ids(path)[-1]
        f = File.get(id=file_id)
        return {
            'st_atime': int(f.accessed_at.timestamp()),
            'st_uid': self._uid,
            'st_gid': self._uid,
            'st_mode': f.mode,
            'st_mtime': int(f.updated_at.timestamp()),
            'st_nlink': 0,
            'st_size': f.size
        }

    @logged
    def readdir(self, path, fh):
        file_id = self._get_path_ids(path)[-1]
        f = File.get(id=file_id)

        dirents = ['.', '..']
        if f.is_directory:
            for each in f.files:
                dirents.append(each.name)
        for r in dirents:
            yield r

    @logged
    def readlink(self, path):
        file_id = self._get_path_ids(path)[-1]
        f = File.get(id=file_id)
        if f.sym_link.startswith("/"):
            return path + f.sym_link
        else:
            return f.sym_link

    @logged
    def mknod(self, path, mode, dev):
        return None

    @logged
    def rmdir(self, path):
        file_id = self._get_path_ids(path)[-1]
        f = File.get(id=file_id)
        if not f.is_directory:
            raise FuseOSError(errno.ENOTDIR)
        elif f.files.count() > 0:
            raise FuseOSError(errno.ENOTEMPTY)
        else:
            f.delete().where(File.id == file_id).execute()

    @logged
    def mkdir(self, path, mode):
        dirs = path.split('/')
        subpath = '/'.join(dirs[:-1])
        new_dir = dirs[-1]
        file_id = self._get_path_ids(subpath)[-1]
        f = File.get(id=file_id)
        File(
            name=new_dir,
            telegram_file_id=None,
            parent_file=f.id,
            size=512,
            user_owner=self._uid,
            group_owner=self._gid,
            is_directory=True,
            mode=16877
        ).save()

    @logged
    def statfs(self, path):
        return {
            # 'f_bavail': -1,  # Free blocks available to unprivileged user
            # 'f_blocks': -1,  # Total data blocks in filesystem
            'f_bsize': 8192,  # Optimal transfer block size
            # 'f_ffree': -1,  # Free file nodes in filesystem
            # 'f_files': -1,  # Total file nodes in filesystem
            # 'f_frsize': 1024,  # Fragment size (since Linux 2.6)
            # 'f_bfree': -1,  # Free blocks in filesystem
            'f_namemax': 255  # Maximum length of filenames
         }

    @logged
    def unlink(self, path):
        file_id = self._get_path_ids(path)[-1]
        File.delete().where(File.id == file_id).execute()

    @logged
    def symlink(self, src, dst):
        file_id = self._get_path_ids(src)[-1]
        File.update(sym_link=dst).where(File.id == file_id).execute()

    @logged
    def rename(self, old, new):  # Also mv
        dir_ids_org = self._get_path_ids(old)

        dirs_new = new.split('/')
        subpath_new = '/'.join(dirs_new)[:-1]
        dir_ids_new = self._get_path_ids(subpath_new)
        name_new = dirs_new[-1]

        f_org = File.get(id=dir_ids_org[-1])
        File.update(
            name=name_new,
            parent_file=dir_ids_new[-1]
        ).where(File.id == f_org.id).execute()

    # def link(self, target, name):
    #     return os.link(self._full_path(target), self._full_path(name))

    @logged
    def utimens(self, path, times=None):
        if times is not None:
            access_time, modified_time = times
            file_id = self._get_path_ids(path)[-1]
            File.update(
                updated_at=datetime.datetime.fromtimestamp(modified_time),
                accessed_at=datetime.datetime.fromtimestamp(access_time)
            ).where(File.id == file_id).execute()

    # File methods
    # ============

    @logged
    def open(self, path, flags):
        return self._get_path_ids(path)[-1]

    @logged
    def create(self, path, mode, fi=None):
        dirs = path.split('/')
        subpath = '/'.join(dirs[:-1])
        filename = dirs[-1]
        file_id = self._get_path_ids(subpath)[-1]
        return File(
            name=filename,
            parent_file=file_id,
            size=0,
            mode=33188,
            is_directory=False,
        ).save()

    @logged
    def read(self, path, length, offset, fh):
        file_id = self._get_path_ids(path)[-1]
        f = File.get(id=file_id)
        telegram_ids = {}

        for telegram_file in f.telegram_files:
            telegram_ids[telegram_file.telegram_id] = telegram_file.file_no

        telegram_ids = sorted(telegram_ids, key=telegram_ids.get)

        telegram_files = []
        for telegram_id in telegram_ids:
            telegram_files.append(self._get_file(telegram_id))

        temp = tempfile.SpooledTemporaryFile(max_size=1024**3)
        for telegram_file in telegram_files:
            file_utils.join(telegram_files, temp)

        temp.seek(offset)
        f.accessed_at = datetime.datetime.now()
        f.save()
        return temp.read(length)

    @logged
    def write(self, path, buf, offset, fh):
        file_id = self._get_path_ids(path)[-1]
        f = File.get(id=file_id)
        file_no = 1 + int(( f.size - offset ) / self._tgchunk_size )
        nof_files = len(buf) / self._tgchunk_size

        telegram_documents = TelegramDocument.select()\
                                .where(
                                    TelegramDocument.file_id == f.id,
                                    TelegramDocument.file_no.between(file_no, file_no + nof_files)
                                )
        print('Get documents for file', f.id, 'and NOs between', file_no, 'and', file_no + nof_files)
        telegram_documents = [{
                'db': t,
                'out': tempfile.SpooledTemporaryFile(max_size=1024**3)
            } for t in telegram_documents]

        if len(telegram_documents) > 0:
            first_file = self._get_file(telegram_documents[0]['db'].telegram_id)
            first_offset = ( f.size - offset ) % self._tgchunk_size
            for data in first_file:
                telegram_documents[0]['out'].write(data)
        else:
            telegram_documents = [{
                'db': TelegramDocument.create(telegram_id=None, file_id=f.id, file_no=0),
                'out': tempfile.SpooledTemporaryFile(max_size=1024**3)
            }]
            first_offset = offset
        telegram_documents[0]['out'].seek(first_offset)
        data_cursor = min(self._tgchunk_size - first_offset, len(buf))
        telegram_documents[0]['out'].write(buf[0:data_cursor])

        for i in range(0, len(telegram_documents)):
            inc = min(data_cursor + self._tgchunk_size, len(buf))
            telegram_documents[i]['out'].write(buf[data_cursor:data_cursor+inc])
            data_cursor = data_cursor + inc

        for i in range(0, len(telegram_documents)):
            new_telegram_id = self._upload_file(telegram_documents[i]['out'])
            telegram_documents[i]['db'].telegram_id = new_telegram_id
            telegram_documents[i]['db'].save()


        f.updated_at = datetime.datetime.now()
        f.size = len(buf)
        f.save()
        return len(buf)

    @logged
    def truncate(self, path, length, fh=None):
        return
        file_id = self._get_path_ids(path)[-1]
        f = File.get(id=file_id)
        telegram_files = [t for t in f.telegram_files]
        nof_chunks = length / self._tgchunk_size
        if len(telegram_files) < nof_chunks:
            return

        TelegramDocument.delete().where(
            TelegramDocument.file_id == f.id,
            TelegramDocument.file_no > nof_chunks
        ).execute()
        last_file = TelegramDocument.get(file_id=f.id, file_no=nof_chunks)
        file_data = self._get_file(last_file.telegram_id)
        buf = tempfile.SpooledTemporaryFile(max_size=1024**3)
        for data in file_data:
            buf.write(data)
        new_telegram_id = self._upload_file(buf)
        last_file.telegram_id = new_telegram_id
        last_file.save()
        f.size = length
        f.save()

    # @logged
    # def flush(self, path, fh):
    #     return os.fsync(fh)

    # @logged
    # def release(self, path, fh):
    #     return os.close(fh)

    # @logged
    # def fsync(self, path, fdatasync, fh):
    #     return self.flush(path, fh)


def main(mountpoint, tg_token, chat_id):
    FUSE(Grameler(tg_token, chat_id), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])
