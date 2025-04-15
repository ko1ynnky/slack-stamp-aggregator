# slack_reaction_aggregator.py

# 必要なライブラリのインポート
import concurrent.futures
import random
import sqlite3
import time
from collections import Counter
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from slack_sdk import WebClient  # slack_sdkが必要 pip install slack_sdk
from slack_sdk.errors import SlackApiError

# dotenvは環境変数からトークンを読み込む場合に必要
# pip install python-dotenv
# from dotenv import load_dotenv


# --- 定数 ---
DB_PATH = "slack_reactions.db"
DEFAULT_DAYS = 365
BATCH_INSERT_SIZE = 100
PROGRESS_UPDATE_INTERVAL = 200
BASE_SLEEP_TIME = 1.2
MAX_WORKERS = 8
MAX_RETRIES = 5

# --- データベース関連関数 ---


def init_database(db_path: str = DB_PATH):
    """
    データベースとテーブルを初期化する関数 (元のスキーマに近づける)
    """
    with sqlite3.connect(db_path) as conn:
        c = conn.cursor()
        # progressテーブル (元のカラム名 last_thread_ts に戻す)
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS progress (
                channel_id TEXT,
                last_message_ts TEXT,
                last_thread_ts TEXT, -- カラム名を元に戻す
                is_completed BOOLEAN DEFAULT 0,
                started_at TIMESTAMP,
                updated_at TIMESTAMP,
                PRIMARY KEY (channel_id)
            )
            """
        )
        # channelsテーブル
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS channels (
                channel_id TEXT PRIMARY KEY,
                channel_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        # messagesテーブル (元の is_reply, parent_message_id を追加)
        # message_id は channel_id + timestamp で生成する運用は維持
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                message_id TEXT PRIMARY KEY,   -- channel_id + ts
                channel_id TEXT,
                user_id TEXT,
                timestamp TEXT UNIQUE,         -- Slackのメッセージタイムスタンプ
                is_reply BOOLEAN,              -- 元のスキーマに合わせる
                parent_message_id TEXT,        -- 元のスキーマに合わせる (FKは付けない)
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (channel_id) REFERENCES channels (channel_id)
            )
            """
        )
        # reactionsテーブル (変更なし)
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS reactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT,
                reaction_name TEXT,
                user_id TEXT,
                timestamp TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (message_id) REFERENCES messages (message_id),
                UNIQUE(message_id, reaction_name, user_id)
            )
            """
        )
        conn.commit()


def get_progress(conn: sqlite3.Connection, channel_id: str) -> Optional[Dict]:
    """
    チャンネルの処理進捗状況を取得 (progressテーブルのカラム名変更に対応)
    """
    c = conn.cursor()
    # last_processed_thread_ts -> last_thread_ts に変更
    c.execute(
        """SELECT last_message_ts, last_thread_ts, is_completed
           FROM progress WHERE channel_id = ?""",
        (channel_id,),
    )
    result = c.fetchone()
    if result:
        return {
            "last_message_ts": result[0],
            "last_thread_ts": result[1],  # カラム名変更
            "is_completed": bool(result[2]),
        }
    return None


def update_progress(
    conn: sqlite3.Connection,
    channel_id: str,
    last_message_ts: Optional[str] = None,
    last_thread_ts: Optional[str] = None,  # カラム名変更
    is_completed: bool = False,
):
    """
    進捗状況を更新 (progressテーブルのカラム名変更に対応)
    """
    c = conn.cursor()
    now = datetime.now()
    c.execute(
        # last_processed_thread_ts -> last_thread_ts に変更
        """
        INSERT INTO progress
        (channel_id, last_message_ts, last_thread_ts, is_completed, started_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(channel_id) DO UPDATE SET
            last_message_ts = COALESCE(excluded.last_message_ts, last_message_ts),
            last_thread_ts = COALESCE(excluded.last_thread_ts, last_thread_ts), -- カラム名変更
            is_completed = excluded.is_completed,
            updated_at = excluded.updated_at
    """,
        (
            channel_id,
            last_message_ts,
            last_thread_ts,  # カラム名変更
            is_completed,
            now,
            now,
        ),
    )
    conn.commit()


def batch_insert_data(
    conn: sqlite3.Connection,
    messages_data: List[Tuple],
    reactions_data: List[Tuple],
):
    """
    メッセージとリアクションデータをバッチで挿入する関数 (messagesテーブルのスキーマ変更に対応)
    """
    c = conn.cursor()
    try:
        # メッセージ情報の一括挿入 (スキーマ変更に合わせてタプルの要素数を変更)
        if messages_data:
            # message_id, channel_id, user_id, timestamp, is_reply, parent_message_id
            c.executemany(
                """
                INSERT OR IGNORE INTO messages
                (message_id, channel_id, user_id, timestamp, is_reply, parent_message_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                messages_data,
            )

        # リアクション情報の一括挿入 (変更なし)
        if reactions_data:
            c.executemany(
                """
                INSERT OR IGNORE INTO reactions
                (message_id, reaction_name, user_id, timestamp)
                VALUES (?, ?, ?, ?)
            """,
                reactions_data,
            )
        conn.commit()
    except sqlite3.Error as e:
        print(f"データベースバッチ書き込みエラー: {e}")
        conn.rollback()


# --- Slack APIエラーハンドリング関数 (変更なし) ---


def handle_rate_limit(error: SlackApiError, context: str = "") -> float:
    retry_after = float(error.response.headers.get("Retry-After", 10))
    jitter = random.uniform(retry_after * 0.1, retry_after * 0.3)
    wait_time = retry_after + jitter
    max_wait = 180
    wait_time = min(wait_time, max_wait)
    print(
        f"{context} レート制限に達しました。{wait_time:.1f}秒後にリトライします..."
    )
    return wait_time


def handle_slack_error(error: SlackApiError, context: str) -> bool:
    error_code = error.response["error"]
    if error_code == "ratelimited":
        wait_time = handle_rate_limit(error, context)
        time.sleep(wait_time)
        print(
            f"{context} レート制限による待機が終了しました。処理を再開します。"
        )
        return True
    elif error_code in ["timeout", "service_unavailable", "fatal_error"]:
        wait_time = random.uniform(10, 30)
        print(
            f"{context}: {error_code}. {wait_time:.1f}秒後にリトライします..."
        )
        time.sleep(wait_time)
        print(f"{context} 待機が終了しました。処理を再開します。")
        return True
    elif error_code in [
        "channel_not_found",
        "is_archived",
        "not_in_channel",
        "access_denied",
        "invalid_auth",
        "account_inactive",
        "token_revoked",
    ]:
        print(f"{context}: スキップまたは停止します ({error_code})")
        return False
    elif error_code == "thread_not_found":
        print(
            f"{context}: スレッドが見つかりません ({error_code})。このスレッドをスキップします。"
        )
        return False
    else:
        print(f"{context}: 未知のエラーが発生しました ({error_code})")
        return False


# --- スコープチェック関数 (変更なし) ---


def check_required_scopes(client: WebClient) -> Tuple[bool, List[str]]:
    try:
        response = client.auth_test()
        granted_scopes = response.headers.get("x-oauth-scopes", "").split(",")
        required_scopes = {
            "channels:history",
            "groups:history",
            "reactions:read",
        }
        missing_scopes = required_scopes - set(granted_scopes)
        if missing_scopes:
            print(
                f"警告: 必要なスコープが不足している可能性があります: {missing_scopes}"
            )
        return True, list(missing_scopes)
    except SlackApiError as e:
        print(f"スコープチェック(auth.test)でエラー: {e.response['error']}")
        if e.response["error"] in ["invalid_auth", "not_authed"]:
            return False, list(required_scopes)
        return True, []


# --- メイン処理関数 ---


def fetch_channel_reactions(
    client: WebClient,  # 型ヒントを WebClient に変更 (より具体的)
    channel_id: str,
    days: int = DEFAULT_DAYS,  # DEFAULT_DAYSを使用
    db_path: str = DB_PATH,  # DB_PATHを使用
) -> Dict[str, int]:  # 返り値の型を Dict[str, int] に戻す
    """
    指定したチャンネルの直近N日間のメッセージに付けられたリアクションを集計し、DBに保存する関数
    (DB接続はこの関数内で管理。messagesテーブルスキーマ変更に対応。)
    """
    print(f"チャンネル {channel_id} の処理を開始します...")
    reaction_counter = Counter()
    processed_message_count = 0
    processed_reactions_count = 0

    with sqlite3.connect(db_path, timeout=10) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")

        # チャンネル情報を channels テーブルに保存 (オプション)
        try:
            conn.execute(
                "INSERT OR IGNORE INTO channels (channel_id) VALUES (?)",
                (channel_id,),
            )
            conn.commit()
        except sqlite3.Error as e:
            print(f"チャンネル {channel_id} の情報保存に失敗(SQLite): {e}")

        # 進捗状況の確認
        progress = get_progress(conn, channel_id)
        if progress and progress["is_completed"]:
            print(
                f"チャンネル {channel_id} は既に処理済みです。スキップします。"
            )
            return dict(reaction_counter)  # dictで返す

        # 集計期間と再開位置の設定
        oldest_ts_limit = (datetime.now() - timedelta(days=days)).timestamp()
        oldest_ts = oldest_ts_limit
        # last_thread_ts カラム名変更に合わせて修正
        last_processed_thread_ts_from_db = None
        if progress and progress["last_message_ts"]:
            oldest_ts = max(
                float(progress["last_message_ts"]), oldest_ts_limit
            )
            last_processed_thread_ts_from_db = progress.get(
                "last_thread_ts"
            )  # カラム名変更
            print(
                f"チャンネル {channel_id} の処理を再開します (from ts: {oldest_ts}, last_thread_ts: {last_processed_thread_ts_from_db})。"
            )

        # DB書き込み用リスト
        messages_to_insert = []
        reactions_to_insert = []

        message_cursor = None
        retry_count = 0
        current_last_message_ts = None  # ページ内の最新TS保持用

        try:
            while True:
                try:
                    history_response = client.conversations_history(
                        channel=channel_id,
                        oldest=str(oldest_ts),
                        limit=200,
                        cursor=message_cursor,
                    )
                    messages = history_response.get("messages", [])
                    if not messages:
                        print(
                            f"チャンネル {channel_id}: 新しいメッセージはありませんでした。"
                        )
                        break

                    # このページの最新メッセージTSを記録 (進捗更新用)
                    current_last_message_ts = messages[0]["ts"]

                    for message in messages:
                        message_ts = message["ts"]
                        user_id = message.get("user")
                        thread_ts = message.get("thread_ts")
                        # 元のスキーマに合わせて is_reply, parent_message_id を設定
                        is_reply = (
                            thread_ts is not None and thread_ts != message_ts
                        )
                        parent_message_id = None
                        if is_reply:
                            parent_message_id = (
                                f"{channel_id}-{thread_ts}"  # 親メッセージのID
                            )

                        message_id = (
                            f"{channel_id}-{message_ts}"  # メッセージID生成
                        )

                        # メッセージ本体のリアクションを処理
                        if "reactions" in message:
                            # messagesテーブル用データ (スキーマ変更に合わせて要素追加)
                            messages_to_insert.append(
                                (
                                    message_id,
                                    channel_id,
                                    user_id,
                                    message_ts,
                                    is_reply,
                                    parent_message_id,
                                )
                            )
                            for reaction in message["reactions"]:
                                emoji_name = reaction["name"]
                                reaction_counter[emoji_name] += reaction[
                                    "count"
                                ]
                                for user in reaction["users"]:
                                    # reactionsテーブル用データ (message_id を使用)
                                    reactions_to_insert.append(
                                        (
                                            message_id,
                                            emoji_name,
                                            user,
                                            message_ts,
                                        )
                                    )
                                    processed_reactions_count += 1

                        # スレッド処理 (is_reply フラグを使わず、元のロジックを維持)
                        if (
                            message.get("reply_count", 0) > 0
                            and message_ts == thread_ts
                        ):
                            should_process_thread = True
                            if last_processed_thread_ts_from_db:
                                if float(thread_ts) <= float(
                                    last_processed_thread_ts_from_db
                                ):
                                    should_process_thread = False

                            if should_process_thread:
                                replies_cursor = None
                                thread_retry_count = 0
                                print(
                                    f"  スレッド {thread_ts} を処理中... (チャンネル: #{channel_id})"
                                )

                                while True:  # スレッド内ページネーション
                                    try:
                                        replies_response = (
                                            client.conversations_replies(
                                                channel=channel_id,
                                                ts=thread_ts,
                                                limit=200,
                                                cursor=replies_cursor,
                                            )
                                        )
                                        reply_messages = replies_response.get(
                                            "messages", []
                                        )
                                        for reply in reply_messages[
                                            1:
                                        ]:  # 親メッセージ除く
                                            reply_ts = reply["ts"]
                                            reply_user_id = reply.get("user")
                                            reply_message_id = (
                                                f"{channel_id}-{reply_ts}"
                                            )
                                            reply_parent_message_id = (
                                                f"{channel_id}-{thread_ts}"
                                            )

                                            if "reactions" in reply:
                                                # messagesテーブル用データ (スレッド返信)
                                                messages_to_insert.append(
                                                    (
                                                        reply_message_id,
                                                        channel_id,
                                                        reply_user_id,
                                                        reply_ts,
                                                        True,
                                                        reply_parent_message_id,
                                                    )
                                                )
                                                for reaction in reply[
                                                    "reactions"
                                                ]:
                                                    emoji_name = reaction[
                                                        "name"
                                                    ]
                                                    reaction_counter[
                                                        emoji_name
                                                    ] += reaction["count"]
                                                    for user in reaction[
                                                        "users"
                                                    ]:
                                                        reactions_to_insert.append(
                                                            (
                                                                reply_message_id,
                                                                emoji_name,
                                                                user,
                                                                reply_ts,
                                                            )
                                                        )
                                                        processed_reactions_count += (
                                                            1
                                                        )

                                        replies_cursor = replies_response.get(
                                            "response_metadata", {}
                                        ).get("next_cursor")
                                        if not replies_cursor:
                                            # スレッド完了 -> last_thread_ts で進捗更新
                                            update_progress(
                                                conn,
                                                channel_id,
                                                last_thread_ts=thread_ts,
                                            )
                                            print(
                                                f"  スレッド {thread_ts} 完了。"
                                            )
                                            break  # スレッドループ終了
                                        time.sleep(BASE_SLEEP_TIME)
                                    except SlackApiError as e_thread:
                                        print(
                                            f"    スレッド {thread_ts} 取得中にエラー発生: {e_thread.response['error']}"
                                        )
                                        if handle_slack_error(
                                            e_thread,
                                            f"スレッド {thread_ts} の取得中",
                                        ):
                                            thread_retry_count += 1
                                            if (
                                                thread_retry_count
                                                < MAX_RETRIES
                                            ):
                                                continue
                                        print(
                                            f"    スレッド {thread_ts} の処理を中断します。"
                                        )
                                        break
                                    except Exception as e_generic_thread:
                                        print(
                                            f"    スレッド {thread_ts} 処理中に予期せぬエラー: {e_generic_thread}"
                                        )
                                        break

                        processed_message_count += 1
                        if (
                            processed_message_count % PROGRESS_UPDATE_INTERVAL
                            == 0
                        ):
                            print(
                                f"チャンネル {channel_id}: {processed_message_count} 件処理済み..."
                            )
                            if messages_to_insert or reactions_to_insert:
                                print(
                                    f"  DBへバッチ書き込み ({len(messages_to_insert)} msgs, {len(reactions_to_insert)} reactions)..."
                                )
                                batch_insert_data(
                                    conn,
                                    messages_to_insert,
                                    reactions_to_insert,
                                )
                                messages_to_insert.clear()
                                reactions_to_insert.clear()
                            # 進捗DB更新 (メッセージTSのみ)
                            update_progress(
                                conn,
                                channel_id,
                                last_message_ts=current_last_message_ts,
                            )

                    message_cursor = history_response.get(
                        "response_metadata", {}
                    ).get("next_cursor")
                    if not message_cursor:
                        print(
                            f"チャンネル {channel_id}: 全メッセージ履歴を取得完了。"
                        )
                        break
                    retry_count = 0
                    time.sleep(BASE_SLEEP_TIME)
                except SlackApiError as e_hist:
                    print(
                        f"チャンネル {channel_id} 履歴取得中にエラー発生: {e_hist.response['error']}"
                    )
                    if handle_slack_error(
                        e_hist, f"チャンネル {channel_id} の履歴取得中"
                    ):
                        retry_count += 1
                        if retry_count < MAX_RETRIES:
                            continue
                    print(f"チャンネル {channel_id} の処理を中断します。")
                    raise e_hist
                except Exception as e_generic_hist:
                    print(
                        f"チャンネル {channel_id} 履歴取得中に予期せぬエラー: {e_generic_hist}"
                    )
                    raise e_generic_hist

            # ループ終了後処理
            print(
                f"チャンネル {channel_id}: ループ終了。最終データ書き込みと進捗更新..."
            )
            if messages_to_insert or reactions_to_insert:
                print(
                    f"  最終DBバッチ書き込み ({len(messages_to_insert)} msgs, {len(reactions_to_insert)} reactions)..."
                )
                batch_insert_data(
                    conn, messages_to_insert, reactions_to_insert
                )
            # 完了状態をDBに記録
            update_progress(
                conn,
                channel_id,
                last_message_ts=current_last_message_ts,
                is_completed=True,
            )
            print(f"チャンネル {channel_id} の処理が正常に完了しました。")
            print(f"  合計処理メッセージ数: {processed_message_count}")
            print(
                f"  合計処理リアクション数（延べ）: {processed_reactions_count}"
            )

        except Exception as e_outer:
            print(
                f"チャンネル {channel_id} の処理中に回復不能なエラーが発生したため中断します: {e_outer}"
            )
            if (
                current_last_message_ts
            ):  # 可能な限り最後のメッセージTSで進捗を保存
                update_progress(
                    conn,
                    channel_id,
                    last_message_ts=current_last_message_ts,
                    is_completed=False,
                )

    # 返り値を Dict[str, int] に合わせる
    return dict(reaction_counter)


def aggregate_channel_reactions(
    client: WebClient,  # 型ヒント追加
    channel_ids: List[str],
    days: int = DEFAULT_DAYS,  # DEFAULT_DAYSを使用
    # db_path は fetch_channel_reactions 内で使用するため、引数からは削除 (元のシグネチャに合わせるなら残す)
    # max_workers は内部で使用するため引数からは削除 (元のシグネチャに合わせる)
) -> Dict[
    str, Dict[str, int]
]:  # 返り値の型を Dict[str, Dict[str, int]] に戻す
    """
    複数チャンネルのリアクションを並列処理で集計する関数
    (元のシグネチャに合わせるため、db_path, max_workers を内部定数化)
    """
    # 内部で使用する定数
    db_path = DB_PATH
    max_workers = MAX_WORKERS

    print("データベースを初期化します...")
    init_database(db_path)

    # 権限チェックはオプションとしてコメントアウト
    # try:
    #     check_required_scopes(client)
    # except Exception as e:
    #     print(f"スコープチェック中にエラー: {e}")

    print(
        f"{len(channel_ids)} チャンネルの処理を最大 {max_workers} スレッドで開始します..."
    )
    # 返り値の型に合わせて results の型ヒントを修正
    results: Dict[str, Dict[str, int]] = {}

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:
        future_to_channel = {
            executor.submit(
                fetch_channel_reactions, client, channel_id, days, db_path
            ): channel_id
            for channel_id in channel_ids
        }

        for future in concurrent.futures.as_completed(future_to_channel):
            channel_id = future_to_channel[future]
            try:
                # fetch_channel_reactions は Dict[str, int] を返す
                channel_result: Dict[str, int] = future.result()
                results[channel_id] = channel_result
                print(
                    f"チャンネル {channel_id} の処理が正常に完了 (取得リアクション種類: {len(channel_result)})"
                )
            except Exception as exc:
                print(
                    f"チャンネル {channel_id} の処理中にエラーが発生しました: {exc}"
                )
                results[channel_id] = {}  # エラー時は空の辞書を入れる

    print("\n--- 全チャンネルの処理が完了しました ---")
    # エラーカウントは簡略化
    error_count = sum(1 for r in results.values() if not r)
    print(f"正常に完了/一部完了したチャンネル数: {len(results) - error_count}")
    if error_count > 0:
        print(
            f"エラーが発生またはリアクションがなかったチャンネル数: {error_count}"
        )

    # 全体合計 (_total_) は含めずに返す
    return results


def get_channel_processing_status(
    db_path: str = DB_PATH, channels_db_path: str = "slack_channels.db"
) -> Tuple[List[Dict], List[Dict]]:
    """
    チャンネルの処理状況を取得する関数

    Args:
        db_path: リアクション集計データベースのパス
        channels_db_path: チャンネル情報データベースのパス

    Returns:
        Tuple[List[Dict], List[Dict]]: (完了チャンネル情報リスト, 処理中チャンネル情報リスト)
        各リストの要素は以下の形式の辞書:
        {
            'channel_id': str,
            'channel_name': str,
            'reaction_count': int,
            'last_updated': str
        }
    """
    # チャンネル名の取得
    channel_names = {}
    try:
        with sqlite3.connect(channels_db_path) as channels_conn:
            channels_cursor = channels_conn.cursor()
            channels_cursor.execute(
                "SELECT channel_id, channel_name FROM channels"
            )
            channel_names = dict(channels_cursor.fetchall())
    except sqlite3.Error as e:
        print(f"チャンネル名の取得中にエラーが発生しました: {e}")

    with sqlite3.connect(db_path) as conn:
        c = conn.cursor()

        # 完了したチャンネルの情報を取得
        completed_query = """
        SELECT 
            c.channel_id,
            COUNT(DISTINCT r.id) as reaction_count,
            datetime(p.updated_at) as last_updated
        FROM channels c
        JOIN progress p ON c.channel_id = p.channel_id
        LEFT JOIN messages m ON c.channel_id = m.channel_id
        LEFT JOIN reactions r ON m.message_id = r.message_id
        WHERE p.is_completed = 1
        GROUP BY c.channel_id
        ORDER BY reaction_count DESC
        """

        # 処理中のチャンネルの情報を取得
        in_progress_query = """
        SELECT 
            c.channel_id,
            COUNT(DISTINCT r.id) as reaction_count,
            datetime(p.updated_at) as last_updated
        FROM channels c
        JOIN progress p ON c.channel_id = p.channel_id
        LEFT JOIN messages m ON c.channel_id = m.channel_id
        LEFT JOIN reactions r ON m.message_id = r.message_id
        WHERE p.is_completed = 0
        GROUP BY c.channel_id
        ORDER BY p.updated_at DESC
        """

        completed_channels = [
            {
                "channel_id": row[0],
                "channel_name": channel_names.get(row[0], "Unknown"),
                "reaction_count": row[1],
                "last_updated": row[2],
            }
            for row in c.execute(completed_query)
        ]

        in_progress_channels = [
            {
                "channel_id": row[0],
                "channel_name": channel_names.get(row[0], "Unknown"),
                "reaction_count": row[1],
                "last_updated": row[2],
            }
            for row in c.execute(in_progress_query)
        ]

        return completed_channels, in_progress_channels


# --- モジュールとして使用されるため、if __name__ == "__main__": ブロックは削除 ---
