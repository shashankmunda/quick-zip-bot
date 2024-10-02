from typing import Iterator
from asyncio import wait
import logging
import asyncio
from asyncio.tasks import FIRST_COMPLETED
from zipfile import ZipFile
from pathlib import Path

from telethon.tl.custom import Message


async def download_files(
    msgs: list[Message],
    conc_max: int = 3,
    root: Path | None = None
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
                if msg.grouped_id:
                    grouped_msgs = await _get_media_posts_in_group(msg.chat_id,msg)
                    for grouped_msg in grouped_msgs:
                        logging.info(f'Downloading {grouped_msg.file.name}')
                        pending.add(asyncio.create_task(msg.download_media(file=root / (grouped_msg.file.name or 'no_name'))))
                        next_msg_index += 1  
                else:
                    logging.info(f'Downloading {msg.file.name}')
                    pending.add(asyncio.create_task(msg.download_media(file=root / (msg.file.name or 'no_name'))))
                    next_msg_index += 1
        
        if pending:
            done, pending = await wait(pending, return_when=FIRST_COMPLETED)

            for task in done:
                try:
                    path = await task
                    if path is not None:
                        yield Path(path)
                except Exception as e:
                    print(f"Error downloading file: {e}")


def add_to_zip(zip: Path, file: Path) -> None:
    """
    Appends a file to a zip file.

    Args:
        zip: the zip file path.
        file: the path to the file that must be added.
    """
    flag = 'a' if zip.is_file() else 'x'
    with ZipFile(zip, flag) as zfile:
        zfile.write(file, file.name)
    file.unlink(True)

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
