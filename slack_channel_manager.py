import os
import sqlite3
import time
from datetime import datetime

from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# 環境変数の読み込み
load_dotenv()


def init_db():
    conn = sqlite3.connect("slack_channels.db")
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS channels (
            channel_id TEXT PRIMARY KEY,
            channel_name TEXT NOT NULL,
            is_private INTEGER NOT NULL,
            is_archived INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    conn.commit()
    return conn


def get_slack_client():
    """Slackクライアントを初期化します"""
    token = os.getenv("SLACK_USER_TOKEN")
    if not token:
        raise ValueError(
            "SLACK_USER_TOKEN not found. Please set it in .env file."
        )
    return WebClient(token=token)


def get_all_channels(client):
    """
    チャンネル一覧を取得します。
    ユーザートークンを使用するため、ユーザーがアクセス可能な
    全てのチャンネルが取得できます。
    """
    channels = []
    cursor = None
    conn = init_db()
    c = conn.cursor()

    while True:
        try:
            response = client.conversations_list(
                types="public_channel,private_channel",
                limit=200,
                cursor=cursor,
            )

            for channel in response["channels"]:
                is_private = (
                    "Yes" if channel.get("is_private", False) else "No"
                )
                is_archived = (
                    "Yes" if channel.get("is_archived", False) else "No"
                )
                print(
                    f"Channel ID: {channel['id']}, "
                    f"Name: {channel['name']}, "
                    f"Private: {is_private}, "
                    f"Archived: {is_archived}"
                )

                if not channel.get("is_archived", False):
                    c.execute(
                        """
                        INSERT INTO channels 
                        (channel_id, channel_name, is_private, is_archived,
                        updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(channel_id) 
                        DO UPDATE SET 
                            channel_name = excluded.channel_name,
                            is_private = excluded.is_private,
                            is_archived = excluded.is_archived,
                            updated_at = excluded.updated_at
                    """,
                        (
                            channel["id"],
                            channel["name"],
                            1 if channel.get("is_private", False) else 0,
                            1 if channel.get("is_archived", False) else 0,
                            datetime.now(),
                        ),
                    )
                    channels.append(channel)

            cursor = response.get("response_metadata", {}).get("next_cursor")
            conn.commit()

            if not cursor:
                break
            time.sleep(1)

        except SlackApiError as e:
            print(f"Error fetching conversations: {e.response['error']}")
            break
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

    conn.close()
    return channels


def show_stored_channels():
    """保存されているアクティブなチャンネル一覧を表示します"""
    conn = sqlite3.connect("slack_channels.db")
    c = conn.cursor()
    print("\nStored channels in database:")
    query = """
        SELECT 
            channel_id, channel_name, is_private, is_archived,
            created_at, updated_at 
        FROM channels
        WHERE is_archived = 0
        ORDER BY channel_name
    """
    for row in c.execute(query):
        is_private = "Yes" if row[2] else "No"
        is_archived = "Yes" if row[3] else "No"
        print(
            f"ID: {row[0]}, "
            f"Name: {row[1]}, "
            f"Private: {is_private}, "
            f"Archived: {is_archived}"
        )
        print(f"Created: {row[4]}, Last Updated: {row[5]}")
    conn.close()
