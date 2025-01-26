from typing import Iterator
from asyncio import wait
from asyncio.tasks import FIRST_COMPLETED
from zipfile import ZipFile
from pathlib import Path
import time,os,psycopg2,logging,asyncio
from telethon import TelegramClient
from dotenv import load_dotenv
from telethon.tl.custom import Message
from psycopg2 import pool

load_dotenv()

# Initialize the connection pool
db_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1, 
    maxconn=10,  # adjust based on your needs
    host=os.environ['DATABASE_HOST'],
    database=os.environ['DATABASE_NAME'],
    user=os.environ['DATABASE_USER'],
    password=os.environ['DATABASE_PASSWORD']
)

# Get a connection from the pool
def get_connection():
    return db_pool.getconn()

# Release the connection back to the pool
def release_connection(conn):
    db_pool.putconn(conn)

def is_admin(user_id):
    return str(user_id) == os.environ['ADMIN_ID']

# Modified function to check if chat is approved
def is_approved_chat(chat_id):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT chat_id FROM approved_chats WHERE chat_id = %s", (chat_id,))
        result = cur.fetchone()
    release_connection(conn)
    return result is not None

def add_approved_chat(chat_id):
    conn = get_connection()
    with conn.cursor() as cur:
        try:
            cur.execute("INSERT INTO approved_chats (chat_id) VALUES (%s) ON CONFLICT DO NOTHING", (chat_id,))
            conn.commit()
        except Exception as e:
            logging.error(f"Error adding chat: {e}")
            conn.rollback()
        finally:
            release_connection(conn)

# Function to remove a chat from the approved list (using PostgreSQL)
def remove_approved_chat(chat_id):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM approved_chats WHERE chat_id = %s", (chat_id,))
        conn.commit()
        release_connection(conn)

async def download_progress_callback(received_bytes: int, total_bytes: int, progress_message: Message, last_message: dict, last_update_time: dict, file_name: str = None):
    """
    Callback function to display download progress and update a message to the user.
    
    Args:
        received_bytes: The number of bytes downloaded so far.
        total_bytes: The total number of bytes to be downloaded.
        progress_message: The message that should be edited to display progress.
        last_message: A dictionary to track the last message content to avoid unnecessary edits.
        last_update_time: A dictionary to track the last update time.
    """
    current_time = time.time()
    progress = (received_bytes / total_bytes) * 100
    bar_length = 20
    filled_length = int(bar_length * received_bytes // total_bytes)
    bar = '■' * filled_length + '□' * (bar_length - filled_length)
    new_message_content = f"\r[{bar}] \n <i>Downloading <b>{file_name}</b> \n {received_bytes/(1024*1024):.2f}/{total_bytes/(1024*1024):.2f} MB ({progress:.2f})%</i>"
    if progress_message and last_message.get('content') != new_message_content and ((current_time - last_update_time.get('time', 0)) >= 10 or progress == 100):
        try:
            await progress_message.edit(new_message_content, parse_mode='html')
            last_message['content'] = new_message_content
            last_update_time['time'] = current_time
            if progress == 100:
                await progress_message.delete()
        except Exception as e:
            logging.error(f"Error updating message: {e}")

async def upload_progress_callback(current_bytes, total_bytes,zipfile, progress_message, last_message, last_update_time):
    """
    Callback function to track and update upload progress.

    Args:
        current: Number of bytes uploaded so far.
        total: Total number of bytes to be uploaded.
        progress_message: The message to be edited with upload progress.
        last_message: Tracks the last message content to avoid unnecessary edits.
    """
    progress = (current_bytes / total_bytes) * 100
    bar_length = 20
    filled_length = int(bar_length * current_bytes // total_bytes)
    bar = '■' * filled_length + '□' * (bar_length - filled_length)
    new_message_content = f"\r[{bar}] \n <i>Uploading <b>{zipfile}</b> \n {current_bytes/(1024*1024):.2f}/{total_bytes/(1024*1024):.2f} MB done ({progress:.2f}%)</i>"
    current_time = time.time()
    # Update message only if content has changed to avoid spamming the API
    if progress_message and last_message.get('content') != new_message_content and ((current_time - last_update_time.get('time', 0)) >= 10 or progress == 100):
        try:
            await progress_message.edit(new_message_content, parse_mode='html')
            last_message['content'] = new_message_content
            last_update_time['time'] = current_time  # Update the last message content
            if progress == 100:
                await progress_message.delete()
        except Exception as e:
            logging.error(f"Error updating message: {e}")

async def upload_files(
        client: TelegramClient, 
        event: any,
        zipfile: any,
        file_title: any
):
    progress_message = await client.send_message(event.sender_id,'Preparing to upload your files...')
    last_message = {'content': ''}
    last_update_time = {'time': 0}
    await client.send_file(
        event.sender_id,
        caption='Done!',
        file=zipfile,
        progress_callback=lambda current, total: upload_progress_callback(current, total, file_title, progress_message, last_message, last_update_time)
    )

async def download_files(
    client: TelegramClient,
    msgs: list[Message],
    conc_max: int = 3,
    root: Path | None = None,
) -> Iterator[Path]:
    """
    Downloads the file if present for each message.

    Args:
        msgs: list of messages from where download the files.
        conc_max: max amount of files to be downloaded concurrently.
        root: root path where store file downloaded.
    
    Returns:
        Yields the path of every file that is downloaded.
    """
    root = root or Path('./')

    next_msg_index = 0
    pending = set()
    while next_msg_index < len(msgs) or pending:
        # fill the pending set with tasks until reach conc_max
        while len(pending) < conc_max and next_msg_index < len(msgs):
            try:
                msg = msgs[next_msg_index]
            except IndexError:
                pass
            else:
                 # Send a new message to the user indicating that the download started
                progress_message = await client.send_message(msg.sender_id, "Download starting...")
                last_message = {'content': ''}
                last_update_time = {'time': 0}

                if msg.grouped_id:
                    grouped_msgs = await _get_media_posts_in_group(msg.chat_id,msg)
                    for grouped_msg in grouped_msgs:
                        logging.info(f'Downloading {grouped_msg.file.name}')
                        while len(pending) >= conc_max:
                            done,pending = await asyncio.wait(pending, return_when= asyncio.ALL_COMPLETED)
                            for task in done:
                                try:
                                    path = await task
                                    if path is None:
                                        yield Path(path)
                                except Exception as e:
                                    logging.error(f"Error downloading file: {e}")
                        pending.add(asyncio.create_task(grouped_msg.download_media(
                            file=root / (grouped_msg.file.name or 'no_name'),
                            progress_callback=lambda received, total, progress_message=progress_message, last_message=last_message, last_update_time=last_update_time, file_name=grouped_msg.file.name: download_progress_callback(received, total, progress_message, last_message, last_update_time, file_name)
                        )))
                    next_msg_index += 1  
                else:
                    logging.info(f'Downloading {msg.file.name}')
                    pending.add(asyncio.create_task(msg.download_media(
                        file=root / (msg.file.name or 'no_name'),
                        progress_callback = lambda received, total, progress_message=progress_message, last_message=last_message, last_update_time=last_update_time, file_name= msg.file.name: download_progress_callback(received, total, progress_message, last_message, last_update_time, file_name)
                        )
                    ))
                    next_msg_index += 1
        
        if pending:
            done, pending = await wait(pending, return_when=FIRST_COMPLETED)

            for task in done:
                try:
                    path = await task
                    if path is not None:
                        yield Path(path)
                except Exception as e:
                    logging.error(f"Error downloading file: {e}")


def add_to_zip(zip_file: Path, file: Path) -> None:
    """
    Appends a file to a zip file.

    Args:
        zip: the zip file path.
        file: the path to the file that must be added.
    """
    flag = 'a' if zip_file.is_file() else 'x'
    try:
        with ZipFile(zip_file, flag) as zfile:
            zfile.write(file, file.name)
        file.unlink(True)
    except PermissionError as e: 
        raise PermissionError(f"Permission denied: {e}")
    except Exception as e: 
        raise RuntimeError(f"An error occured while adding '{file}' to '{zip_file}': {e}")

async def _get_media_posts_in_group(chat, original_post, max_amp=10):
    """
    Searches for Telegram posts that are part of the same group of uploads
    The search is conducted around the id of the original post with an amplitude
    of `max_amp` both ways
    Returns a list of [post] where each post has media and is in the same grouped_id
    """
    if original_post.grouped_id is None:
        return [original_post] if original_post.media is not None else []

    search_ids = [i for i in range(original_post.id - max_amp, original_post.id + max_amp + 1)]
    posts = await original_post.client.get_messages(chat, ids=search_ids)
    media = []
    for post in posts:
        if post is not None and post.grouped_id == original_post.grouped_id and post.media is not None:
            media.append(post)
    return media
