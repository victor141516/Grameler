from __future__ import with_statement

import datetime
import errno
from functools import wraps
from io import BytesIO
import logging
import os
from peewee import fn
import requests
import sys
from threading import Thread
import time

from database import db, File, TelegramDocument
import file_utils
from fuse import FUSE, FuseOSError, Operations
import telebot
import tempfile

logging.basicConfig()
log = logging.getLogger(__name__)

def logged(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            ret = f(*args, **kwargs)
            log.error('-------------------------------------\n%s(%s) = %s', f.__name__, ','.join([str(item) for item in args[1:]]), str(ret))
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
        self.tempfiles = {}
        Thread(target=self.upload_files_daemon).start()


    # Helpers
    # =======

    # @logged
    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path


    # @logged
    def _upload_file(self, doc):
        doc.seek(0)
        return self.tgbot.send_document(self.chat_id, doc).document.file_id

    # @logged
    def _get_file(self, file_id, return_raw=True):
        file_info = self.tgbot.get_file(file_id)
        res = requests.get('https://api.telegram.org/file/bot{0}/{1}'.format(self.tg_token, file_info.file_path), stream=True)
        if return_raw:
            return res.raw
        else:
            return res

    # @logged
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

    # @logged
    def access(self, path, mode):
        self._get_path_ids(path)

    # @logged
    def chmod(self, path, mode):
        file_id = self._get_path_ids(path)[-1]
        File.update(mode=mode).where(File.id == file_id).execute()

    # @logged
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

    # @logged
    def readdir(self, path, fh):
        file_id = self._get_path_ids(path)[-1]
        f = File.get(id=file_id)

        dirents = ['.', '..']
        if f.is_directory:
            for each in f.files:
                dirents.append(each.name)
        for r in dirents:
            yield r

    # @logged
    def readlink(self, path):
        file_id = self._get_path_ids(path)[-1]
        f = File.get(id=file_id)
        if f.sym_link.startswith("/"):
            return path + f.sym_link
        else:
            return f.sym_link

    # @logged
    def mknod(self, path, mode, dev):
        return None

    # @logged
    def rmdir(self, path):
        file_id = self._get_path_ids(path)[-1]
        f = File.get(id=file_id)
        if not f.is_directory:
            raise FuseOSError(errno.ENOTDIR)
        elif f.files.count() > 0:
            raise FuseOSError(errno.ENOTEMPTY)
        else:
            f.delete().where(File.id == file_id).execute()

    # @logged
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
            'f_bavail': 2024,  # Free blocks available to unprivileged user
            'f_blocks': 2024,  # Total data blocks in filesystem
            'f_bsize': 512,  # Optimal transfer block size
            # 'f_ffree': -1,  # Free file nodes in filesystem
            # 'f_files': -1,  # Total file nodes in filesystem
            # 'f_frsize': self._tgchunk_size,  # Fragment size (since Linux 2.6)
            'f_bfree': 2024,  # Free blocks in filesystem
            'f_namemax': 255  # Maximum length of filenames
         }

    # @logged
    def unlink(self, path):
        file_id = self._get_path_ids(path)[-1]
        File.delete().where(File.id == file_id).execute()

    # @logged
    def symlink(self, src, dst):
        file_id = self._get_path_ids(src)[-1]
        File.update(sym_link=dst).where(File.id == file_id).execute()

    # @logged
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

    # @logged
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

    # @logged
    def open(self, path, flags):
        return self._get_path_ids(path)[-1]

    # @logged
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

    # @logged
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

    def upload_files_daemon(self):
        while True:
            temp_copy = self.tempfiles.copy()
            for path in temp_copy:
                if (datetime.datetime.now() - temp_copy[path]['lastwrite']).seconds > 10:
                    print('Uploading file: ' + path)
                    if path in self.tempfiles:
                        del(self.tempfiles[path])
                        self._upload_file(temp_copy[path]['file'])
            time.sleep(5)

    # @logged
    def write(self, path, buf, offset, fh):
        print('Write:')
        print('  offset: ' + str(offset))
        print('  bytes: ' + str(len(buf)))
        if path not in self.tempfiles:
            self.tempfiles[path] = {'file': tempfile.SpooledTemporaryFile(max_size=50*(1024**2))}
        self.tempfiles[path]['lastwrite'] = datetime.datetime.now()
        self.tempfiles[path]['file'].seek(offset)
        print(self.tempfiles[path]['file'].write(buf))
        return


        print('Write:')
        print('  offset: ' + str(offset))
        print('  bytes: ' + str(len(buf)))
        file_id = self._get_path_ids(path)[-1]
        f = File.get(id=file_id)

        nof_chunks = int(len(buf) / self._tgchunk_size) + (1 if len(buf) != self._tgchunk_size else 0)
        first_chunk_start_position = offset - (offset % self._tgchunk_size)
        first_chunk_no = int(first_chunk_start_position / self._tgchunk_size)

        chunks_streams = []
        for x in range(first_chunk_no, first_chunk_no + nof_chunks):
            tgd, created = TelegramDocument.get_or_create(
                file_id=f.id,
                file_no=x
            )
            if created:
                chunks_streams.append(None)
            else:
                chunks_streams.append(self._get_file(tgd.telegram_id))

        for x in range(first_chunk_no, first_chunk_no + nof_chunks):
            chunk_stream = chunks_streams.pop(0)
            chunk_buf = tempfile.SpooledTemporaryFile(max_size=1024**3)
            chunk_buf.seek(0)
            if chunk_stream is not None:
                chunk_buf.write(chunk_stream.read())

            if x is 0:
                start_chunk = offset % self._tgchunk_size
                last_chunk = self._tgchunk_size
                start_buf = start_chunk + (first_chunk_no * self._tgchunk_size)
                last_buf = (first_chunk_no + 1) * self._tgchunk_size
            elif x is (first_chunk_no + nof_chunks - 1):
                start_chunk = 0
                last_chunk = self._tgchunk_size
                start_buf =  x * self._tgchunk_size
                last_buf = ((x + 1) * self._tgchunk_size) - 1
            else:
                start_chunk = 0
                last_chunk = (offset + len(buf)) % self._tgchunk_size
                start_buf = x * self._tgchunk_size
                last_buf = start_buf + last_chunk

            chunk_buf.seek(start_chunk)
            print('  bytes to write (' + str(start_buf) + ':' + str(last_buf) + '): ' + str(len(buf[start_buf:last_buf])))
            bytes_written = chunk_buf.write(buf[start_buf:last_buf])
            print('  bytes written: ' + str(bytes_written))
            # chunk_buf[start_chunk:last_chunk] = buf[start_buf:last_buf]
            tg_doc_id = self._upload_file(chunk_buf)
            tgd.telegram_id = tg_doc_id
            tgd.save()


        last_tgd = TelegramDocument.select(
            fn.MAX(TelegramDocument.file_no),
            TelegramDocument.id,
            TelegramDocument.telegram_id,
            TelegramDocument.file_no
        ).where(TelegramDocument.file_id == f.id).first()
        file_size = last_tgd.file_no * self._tgchunk_size
        file_size += int(self._get_file(last_tgd.telegram_id).headers['Content-length'])
        print('Last file size: ' + self._get_file(last_tgd.telegram_id).headers['Content-length'])

        f.updated_at = datetime.datetime.now()
        f.size = file_size
        f.save()
        print('\n')
        return f.size

    # @logged
    def truncate(self, path, length, fh=None):
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
