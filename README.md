# Grameler

A clasic file system basing its storage on Telegram documents

## Usage

`$ pip install -r requirements.txt`

`$ python grameler.py <DIRECTORY/TO/MOUNT> <TELEGRAM_BOT_TOKEN> <USER_ID>`
`USER_ID` is the same as the chat_id for your own chat with the bot. Grameler in fact needs the `chat_id`.


## TODO

- [ ] Fix write operation
- [ ] Check if support for big files (>20MB) if working
- [ ] Maybe cache (?)
- [ ] Upload database to telegram and store locally only the file_id
