import datetime
from peewee import *

db = SqliteDatabase('grameler.sqlite')

class File(Model):
    name = CharField()
    telegram_file_id = CharField(null=True)
    parent_file = ForeignKeyField('self', backref='files', null=True, default=None)
    size = BigIntegerField(default=512)
    user_owner = IntegerField(default=0)
    group_owner = IntegerField(default=0)
    sym_link = CharField(max_length=2048, null=True, default=None)
    is_directory = BooleanField(default=False)
    mode = IntegerField()
    permission_user_read = BooleanField(default=True)
    permission_user_write = BooleanField(default=True)
    permission_user_exec = BooleanField(default=False)
    permission_group_read = BooleanField(default=True)
    permission_group_write = BooleanField(default=False)
    permission_group_exec = BooleanField(default=False)
    permission_others_read = BooleanField(default=True)
    permission_others_write = BooleanField(default=False)
    permission_others_exec = BooleanField(default=False)
    created_at = DateTimeField(default=datetime.datetime.now)
    updated_at = DateTimeField(default=datetime.datetime.now)
    accessed_at = DateTimeField(default=datetime.datetime.now)

    class Meta:
        database = db


class TelegramDocument(Model):
    telegram_id = CharField(null=True)
    file_id = ForeignKeyField(File, backref='telegram_files')
    file_no = IntegerField(default=0)

    class Meta:
        database = db


db.connect()
db.create_tables([File, TelegramDocument])
