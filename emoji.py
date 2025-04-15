import os
import sys
from typing import Dict, Optional

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# dotenvを使用する場合 (pip install python-dotenv)
# from dotenv import load_dotenv
# load_dotenv()


def get_custom_emojis(client: WebClient) -> Optional[Dict[str, str]]:
    """
    ワークスペースに登録されているカスタム絵文字とその画像URLを取得します。

    Args:
        client: 初期化済みの slack_sdk WebClient インスタンス。

    Returns:
        カスタム絵文字名をキー、画像URLを値とする辞書。
        エイリアス（他の絵文字への参照）は除外されます。
        エラーが発生した場合は None を返します。
    """
    print("カスタム絵文字リストを取得中...")
    custom_emojis: Dict[str, str] = {}
    try:
        # emoji.list APIメソッドを呼び出す
        response = client.emoji_list()

        # API呼び出しが成功したかチェック
        if response["ok"]:
            emoji_data = response["emoji"]
            alias_count = 0
            for name, url_or_alias in emoji_data.items():
                # 値が "alias:" で始まっていないものが実際の画像URL
                if not url_or_alias.startswith("alias:"):
                    custom_emojis[name] = url_or_alias
                else:
                    alias_count += 1
            print(
                f"成功: {len(custom_emojis)} 件のカスタム絵文字を取得しました "
                f"({alias_count} 件のエイリアスは除外)。"
            )
            return custom_emojis
        else:
            # APIがエラーを返した場合
            error_msg = response.get("error", "不明なエラー")
            print(f"絵文字リストの取得に失敗しました (APIエラー): {error_msg}")
            if error_msg == "missing_scope":
                print("  -> Botに必要な権限 'emoji:read' がありません。")
            return None

    except SlackApiError as e:
        # slack_sdkが例外を発生させた場合
        error_code = e.response.get("error", "不明なAPIエラー")
        print(
            f"絵文字リスト取得中にSlack APIエラーが発生しました: {error_code}"
        )
        if error_code == "missing_scope":
            print(
                "  -> Botに必要な権限 'emoji:read' がありません。アプリの権限を確認してください。"
            )
        elif error_code in [
            "invalid_auth",
            "not_authed",
            "account_inactive",
            "token_revoked",
        ]:
            print(
                "  -> 認証に失敗しました。Slack Bot Tokenを確認してください。"
            )
        # 必要に応じてレートリミットハンドリングを追加
        # elif error_code == 'ratelimited':
        #     wait_time = float(e.response.headers.get("Retry-After", 5))
        #     print(f"  -> レート制限に達しました。{wait_time:.1f}秒後に再試行してください。")
        return None
    except Exception as e:
        # その他の予期せぬエラー
        print(f"絵文字リスト取得中に予期せぬエラーが発生しました: {e}")
        return None


# --- 以下、この関数を使用する際のサンプルコード ---
if __name__ == "__main__":

    # ★★★ Slack User Token を設定してください ★★★
    SLACK_USER_TOKEN = os.getenv("SLACK_USER_TOKEN")

    if not SLACK_USER_TOKEN:
        print("エラー: 環境変数 SLACK_USER_TOKEN が設定されていません。")
        sys.exit(1)

    # WebClientを初期化
    slack_client = WebClient(token=SLACK_USER_TOKEN)

    # 接続と権限の基本的なチェック (オプション)
    try:
        auth_test = slack_client.auth_test()
        print(
            f"認証成功: Bot ID={auth_test.get('bot_id')}, Team ID={auth_test.get('team_id')}"
        )
        # ここで auth_test.response_metadata.get('scopes') を使って
        # 'emoji:read' が含まれているか確認することもできます。
        scopes = auth_test.headers.get("x-oauth-scopes", "")
        if "emoji:read" not in scopes:
            print(
                "警告: Botトークンに 'emoji:read' スコープが含まれていないようです。"
            )
            print(f"  現在のスコープ: {scopes}")

    except SlackApiError as e:
        print(f"認証失敗: {e.response['error']}")
        print(
            "SLACK_USER_TOKENが正しいか、アプリがインストールされているか確認してください。"
        )
        exit()

    # カスタム絵文字を取得
    emoji_dictionary = get_custom_emojis(slack_client)

    # 結果を表示
    if emoji_dictionary is not None:
        print("\n取得したカスタム絵文字 (最初の20件):")
        count = 0
        for name, url in emoji_dictionary.items():
            print(f"  :{name}:  => {url}")
            count += 1
            if count >= 20:
                print("  ...")
                break

        # これで emoji_dictionary に {絵文字名: 画像URL} の辞書が格納されています。
        # 例: emoji_dictionary[' Giga_Chad_Approved ']
        # print(f"\n例: ': Giga_Chad_Approved:' のURLは {emoji_dictionary.get(' Giga_Chad_Approved ')}")

    else:
        print("\nカスタム絵文字の取得に失敗しました。")
