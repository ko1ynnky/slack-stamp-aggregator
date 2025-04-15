# Slack リアクション集計ツール

Slack チャンネル内のメッセージに付けられたリアクション（スタンプ）を集計し、SQLite データベースに保存するツールです。
スレッド内のリアクションも含めて集計し、チャンネルごとの利用状況を分析できます。

## 機能

- 指定した Slack チャンネルの過去 1 年分（デフォルト）のメッセージのリアクションを集計
- スレッド内のリアクションも含めて収集
- SQLite データベースに結果を保存
- 処理の中断・再開機能（チェックポイント機能）
- レート制限への自動対応
- 必要な Slack API スコープの自動チェック

## 必要な Slack API スコープ

- channels:history
- channels:read
- groups:history
- groups:read
- reactions:read

## セットアップ

1. 必要なパッケージのインストール:

```bash
pip install slack-sdk
```

2. 環境変数の設定:
   `.env`ファイルをプロジェクトルートに作成し、以下の内容を設定してください：

```
SLACK_TOKEN=xoxp-your-user-token
# または
SLACK_TOKEN=xoxb-your-bot-token
```

## 使用方法

`slack_channel_manager.py` と `slack_reaction_aggregator.py` の 2 つのスクリプトで構成されています。

### 1. チャンネルリストの取得・更新 (`slack_channel_manager.py`)

`slack_channel_manager.py` は、アクセス可能な全てのパブリックチャンネルとプライベートチャンネルの情報を取得し、`slack_channels.db` という SQLite データベースに保存します。
すでに存在するチャンネルは情報を更新し、アーカイブされたチャンネルは DB に保存しません（DB 上は is_archived=0 のまま残ります）。

このスクリプトを実行するには **ユーザートークン (`SLACK_USER_TOKEN`)** が必要です。`.env` ファイルに設定してください。

```dotenv
SLACK_USER_TOKEN=xoxp-your-user-token
```

以下のコマンドで実行します。

```bash
python slack_channel_manager.py
```

実行すると、取得したチャンネル情報がコンソールに表示され、データベース (`slack_channels.db`) が作成・更新されます。

### 2. リアクションの集計 (`slack_reaction_aggregator.py`)

`slack_reaction_aggregator.py` は、指定されたチャンネル ID リストに基づいて、各チャンネルのメッセージ（スレッド含む）のリアクションを集計し、`slack_reactions.db` という SQLite データベースに保存します。
処理の進捗状況も同データベース内の `progress` テーブルに記録され、中断・再開が可能です。

このスクリプトは **ボットトークン (`SLACK_TOKEN`) またはユーザートークン** で実行できます。`.env` ファイルに設定してください。

```dotenv
SLACK_TOKEN=xoxb-your-bot-token
# または
# SLACK_TOKEN=xoxp-your-user-token
```

以下のように Python スクリプトから `aggregate_channel_reactions` 関数を呼び出して使用します。

```python
import sqlite3
from slack_sdk import WebClient
from slack_reaction_aggregator import aggregate_channel_reactions, get_channel_processing_status
import os
from dotenv import load_dotenv

load_dotenv() # .envファイルから環境変数を読み込む

# Slackクライアントの初期化 (環境変数 SLACK_TOKEN を使用)
token = os.getenv("SLACK_TOKEN")
if not token:
    raise ValueError("SLACK_TOKEN not found in environment variables.")
client = WebClient(token=token)

# 1. slack_channels.db から集計対象のチャンネルIDリストを取得
try:
    conn_channels = sqlite3.connect("slack_channels.db")
    c_channels = conn_channels.cursor()
    # アーカイブされていないチャンネルのみを取得
    c_channels.execute("SELECT channel_id FROM channels WHERE is_archived = 0")
    target_channels = [row[0] for row in c_channels.fetchall()]
    conn_channels.close()
    print(f"Found {len(target_channels)} active channels in slack_channels.db")
except sqlite3.Error as e:
    print(f"Error reading from slack_channels.db: {e}")
    target_channels = [] # エラーの場合は空リスト

if not target_channels:
    print("No channels found to process. Run slack_channel_manager.py first.")
else:
    # リアクションの集計実行 (過去365日分)
    results = aggregate_channel_reactions(client, target_channels, days=365)

    # 結果の表示 (例)
    print("\n--- Aggregation Summary ---")
    for channel_id, reactions in results.items():
        if reactions: # リアクションがあったチャンネルのみ表示
            print(f"\nChannel {channel_id}:")
            # 上位5件を表示
            top_reactions = sorted(reactions.items(), key=lambda x: x[1], reverse=True)[:5]
            for emoji, count in top_reactions:
                print(f"  :{emoji}: - {count}")
            if len(reactions) > 5:
                print("  ...")
        # else:
        #     print(f"\nChannel {channel_id}: No reactions found or error occurred.")

    # 処理状況の確認
    print("\n--- Processing Status ---")
    completed, in_progress = get_channel_processing_status()
    print(f"Completed channels: {len(completed)}")
    print(f"In-progress channels: {len(in_progress)}")
    # 詳細表示 (例)
    # for channel in completed:
    #     print(f"  - {channel['channel_name']} ({channel['channel_id']}): {channel['reaction_count']} reactions, Last updated: {channel['last_updated']}")
```

### データベースの構造

`slack_channel_manager.py` は `slack_channels.db` を作成・更新します。
`slack_reaction_aggregator.py` は `slack_reactions.db` を作成・更新します。

**`slack_channels.db`**

- `channels`: チャンネル情報 (ID, 名前, プライベートかどうか, アーカイブ済みかどうか, 作成日時, 更新日時)

**`slack_reactions.db`**

- `channels`: チャンネル情報
- `messages`: メッセージ情報
- `reactions`: リアクション（スタンプ）情報
- `progress`: 処理の進捗状況

### エラーハンドリング

- レート制限に達した場合、自動的に適切な待機時間を設定して再試行
- ネットワークエラーやタイムアウトの場合、ランダムな待機時間後に再試行
- 処理が中断された場合、最後に処理したメッセージから再開可能

## 注意事項

- 大量のチャンネルや長期間のデータを一度に処理する場合、API レート制限に注意してください
- データベースファイル（`slack_reactions.db`）は自動的に作成されます
- 環境変数（`.env`）とデータベースファイルは`.gitignore`に含まれています

## ライセンス

MIT

## 貢献

バグ報告や機能改善の提案は、Issue を作成してください。
プルリクエストも歓迎します。
