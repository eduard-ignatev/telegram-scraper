import argparse
import os
import sqlite3
import sys
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from telethon import TelegramClient

# Load environment variables
load_dotenv()

# Telegram API credentials
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')

# SQLite database setup
DB_NAME = os.getenv('DB_NAME')


def create_db() -> None:
    """Create SQLite database and table if they don't exist."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER,
            message_date TEXT,
            message_text TEXT,
            channel_name TEXT,
            scape_ts TEXT
        )
    ''')
    conn.commit()
    conn.close()

def insert_message(data: tuple) -> None:
    """Insert scraped message into the database."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO messages (message_id, message_date, message_text, channel_name, scape_ts)
        VALUES (?, ?, ?, ?, ?)
    ''', data)
    conn.commit()
    conn.close()

async def scrape_channel(client: TelegramClient, channel_name: str, limit: Optional[int] = None, offset_date: Optional[str] = None) -> None:
    """Scrape messages from a Telegram channel."""
    try:
        messages_cnt = 0
        start_dt = datetime.now()
        async for message in client.iter_messages(channel_name, limit=limit, offset_date=offset_date, reverse=True):
            # Filter messages with empty text
            if message.message:
                try:
                    message_data = (
                        message.id,
                        message.date.isoformat(),
                        message.message,
                        channel_name,
                        datetime.now().isoformat()
                    )
                    insert_message(message_data)
                    messages_cnt += 1
                    sys.stdout.write(f"\rScraping channel: {channel_name} - Messages count: {messages_cnt}")
                    sys.stdout.flush()
                except Exception as e:
                    print(f"Error processing message {message.id}: {e}")
        end_dt = datetime.now()
        print(f'\nElapsed time: {end_dt - start_dt}')
    except ValueError as e:
        print(f"Error with channel {channel_name}: {e}")


if __name__ == '__main__':
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Scrape messages from a Telegram channel.")
    parser.add_argument('channel_name', type=str, help="Username of the channel to scrape.")
    parser.add_argument('--limit', type=int, default=None, help="Limit the number of messages to scrape.")
    parser.add_argument('--offset_date', type=str, default=None, help="Scrape messages after this date (YYYY-MM-DD).")
    args = parser.parse_args()

    # Convert offset_date to datetime object if provided
    offset_date = None
    if args.offset_date:
        offset_date = datetime.strptime(args.offset_date, '%Y-%m-%d')

    # Create database and table
    create_db()

    # Start Telegram client and scrape messages
    client = TelegramClient('session', API_ID, API_HASH)
    with client:
        client.loop.run_until_complete(scrape_channel(client, args.channel_name, args.limit, offset_date))
