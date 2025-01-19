import sqlite3
import sys
from datetime import datetime
from typing import Optional

import yaml
from telethon import TelegramClient


class Database:
    def __init__(self, db_name: str) -> None:
        self.db_name = db_name
        self.connection = None

    def connect(self) -> None:
        self.connection = sqlite3.connect(self.db_name)

    def create_table(self) -> None:
        cursor = self.connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER,
                message_date TEXT,
                message_text TEXT,
                channel_name TEXT,
                scape_ts TEXT
            )
        """)
        self.connection.commit()

    def insert_message(self, data: tuple) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO messages (message_id, message_date, message_text, channel_name, scape_ts)
            VALUES (?, ?, ?, ?, ?)
        """,
            data,
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


async def scrape_channel(
    client: TelegramClient,
    channel_name: str,
    limit: Optional[int] = None,
    offset_date: Optional[str] = None,
    reverse: bool = False,
) -> None:
    """Scrape messages from a Telegram channel."""
    try:
        messages_cnt = 0
        start_dt = datetime.now()
        async for message in client.iter_messages(channel_name, limit=limit, offset_date=offset_date, reverse=reverse):
            # Filter messages with empty text
            if message.message:
                try:
                    message_data = (
                        message.id,
                        message.date.isoformat(),
                        message.message,
                        channel_name,
                        datetime.now().isoformat(),
                    )
                    db.insert_message(message_data)
                    messages_cnt += 1
                    sys.stdout.write(f'\rScraping channel: {channel_name} - Messages count: {messages_cnt}')
                    sys.stdout.flush()
                except Exception as e:
                    print(f'Error processing message {message.id}: {e}')
        end_dt = datetime.now()
        print(f'\nElapsed time: {end_dt - start_dt}')
    except ValueError as e:
        print(f'Error with channel {channel_name}: {e}')


if __name__ == '__main__':
    # Load configuration from YAML file
    with open('config.yaml') as file:
        config = yaml.safe_load(file)

    # Create database and table
    db = Database(db_name=config['db_name'])
    db.connect()
    db.create_table()

    # Start Telegram client and scrape messages
    client = TelegramClient('session', config['api_id'], config['api_hash'])
    with client:
        for channel_name in config['channels']:
            client.loop.run_until_complete(
                scrape_channel(client, channel_name, config['limit'], config['offset_date'], config['reverse'])
            )

    # Close database
    db.close()
