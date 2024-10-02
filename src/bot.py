from functools import partial
from asyncio import get_running_loop
from shutil import rmtree
from pathlib import Path
import logging
import os
import time
import asyncio

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.events import NewMessage, StopPropagation
from telethon.tl.custom import Message

from utils import download_files, add_to_zip

load_dotenv()

API_ID = os.environ['API_ID']
API_HASH = os.environ['API_HASH']
BOT_TOKEN = os.environ['BOT_TOKEN']
CONC_MAX = int(os.environ.get('CONC_MAX', 3))
STORAGE = Path('./files/')

TASK_TIMEOUT = 3600
MAX_SIZE = 1024 * 1024 * 2000  # 2 GB

MessageEvent = NewMessage.Event | Message

logging.basicConfig(
    format='[%(levelname)s/%(asctime)s] %(name)s: %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
    ]
)

# dict to keep track of tasks for every user
tasks: dict[int, dict] = {}

bot = TelegramClient(
    'quick-zip-bot', api_id=API_ID, api_hash=API_HASH
).start(bot_token=BOT_TOKEN)


@bot.on(NewMessage(pattern='/add'))
async def start_task_handler(event: MessageEvent):
    """
    Notifies the bot that the user is going to send the media.
    """
    tasks[event.sender_id] = {
        'message_ids': [],
        'timestamp': time.time()
    }

    await event.respond('OK, send me some files.')

    raise StopPropagation

@bot.on(NewMessage(pattern='/start'))
async def welcome_handler(event: MessageEvent):
    """
    Sends a welcome message to the bot on start.
    """
    await event.respond('Welcome to SM Bot 👋')

    raise StopPropagation


@bot.on(NewMessage(
    func=lambda e: e.sender_id in tasks and e.file is not None))
async def add_file_handler(event: MessageEvent):
    """
    Stores the ID of messages sended with files by this user.
    """
    if event.sender_id not in tasks:
        return
    
    message_size = event.file.size
    current_size = sum(
        [m.file.size for m in await bot.get_messages(event.sender_id, ids=tasks[event.sender_id]['message_ids'])]
    )
    
    if current_size + message_size > MAX_SIZE:
        await event.respond("Adding this file will exceed the 2 GB limit. Please send smaller files.")
        return
    
    if event.sender_id in tasks:
        tasks[event.sender_id]['message_ids'].append(event.id)
        tasks[event.sender_id]['timestamp'] = time.time()  # Update timestamp on each new file

    raise StopPropagation


@bot.on(NewMessage(pattern=r'/zip (?P<name>\w+)'))
async def zip_handler(event: MessageEvent):
    """
    Zips the media of messages corresponding to the IDs saved for this user in
    tasks. The zip filename must be provided in the command.
    """
    if event.sender_id not in tasks:
        await event.respond('You must use /add first.')
        return
    
    elif not tasks[event.sender_id]:
        await event.respond('You must send me some files first.')
        return
    
    else:
        try:
            messages = await bot.get_messages(
                event.sender_id, ids=tasks[event.sender_id]['message_ids'])
            zip_size = sum([m.file.size for m in messages])

            if zip_size > 1024 * 1024 * 2000:   # zip_size > 1.95 GB approximately
                await event.respond('Total filesize don\'t must exceed 2.0 GB.')
                return
            
            root = STORAGE / f'{event.sender_id}/'
            root.mkdir(parents=True, exist_ok=True)
            zip_name = root / (event.pattern_match['name'] + '.zip')

            # Download files and add to zip with error handling
            async for file in download_files(bot,messages, CONC_MAX, root):
                await get_running_loop().run_in_executor(
                    None, partial(add_to_zip, zip_name, file))
            progress_message = await event.respond('Preparing to upload your files...')
            last_message = {'content': ''}
            last_update_time = {'time': 0}
            await bot.send_file(
                event.chat_id,
                caption='Done!',
                file=zip_name,
                progress_callback=lambda current, total: upload_progress_callback(current, total, progress_message, last_message, last_update_time)
            )
            
        except Exception as e:
            logging.error(f"Error during file processing: {e}")
            await event.respond(f"An error occurred: {str(e)}")

        finally:
            # Clean up files after sending or error
            if (STORAGE / str(event.sender_id)).exists():
                await get_running_loop().run_in_executor(
                    None, rmtree, STORAGE / str(event.sender_id))
            tasks.pop(event.sender_id)

    raise StopPropagation


@bot.on(NewMessage(pattern='/cancel'))
async def cancel_handler(event: MessageEvent):
    """
    Cleans the list of tasks for the user.
    """
    try:
        tasks.pop(event.sender_id)
    except KeyError:
        pass

    await event.respond('Canceled zip. For a new one, use /add.')

    raise StopPropagation

# async def clean_old_tasks():
#     """
#     Periodically removes tasks that have exceeded the timeout.
#     """
#     while True:
#         current_time = time.time()
#         for user_id, task_info in list(tasks.items()):
#             if current_time - task_info['timestamp'] > TASK_TIMEOUT:
#                 logging.info(f"Removing expired task for user {user_id}")
#                 tasks.pop(user_id)
#         await asyncio.sleep(60)  # Check every minute

# bot.loop.create_task(clean_old_tasks())

async def upload_progress_callback(current, total, progress_message, last_message, last_update_time):
    """
    Callback function to track and update upload progress.

    Args:
        current: Number of bytes uploaded so far.
        total: Total number of bytes to be uploaded.
        progress_message: The message to be edited with upload progress.
        last_message: Tracks the last message content to avoid unnecessary edits.
    """
    progress = (current / total) * 100
    bar_length = 20
    filled_length = int(bar_length * current // total)
    bar = '■' * filled_length + '□' * (bar_length - filled_length)
    new_message_content = f"\r[{bar}] \n <i>Uploaded {progress:.2f}%</i>"
    current_time = time.time()
    # Update message only if content has changed to avoid spamming the API
    if last_message.get('content') != new_message_content and ((current_time - last_update_time.get('time', 0)) >= 5 or progress == 100):
        try:
            await progress_message.edit(new_message_content, parse_mode='html')
            if progress == 100:
                await progress_message.delete()
            last_message['content'] = new_message_content
            last_update_time['time'] = current_time  # Update the last message content
        except Exception as e:
            print(f"Error updating message: {e}")

if __name__ == '__main__':
    bot.run_until_disconnected()
