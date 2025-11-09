import json
import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv # [追加] .env ファイルを読み込むライブラリ

# [追加] .env ファイルから環境変数を読み込む
# このスクリプト(db_loader.py)がsrc/にあるので、.envは一つ上の階層(../)にある想定
dotenv_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(dotenv_path=dotenv_path)
print(f".env ファイル ({dotenv_path}) から環境変数を読み込みました。") # デバッグ用

# --- ファイル設定 ---
INPUT_JSON_FILE = '../data/output/analysis_results.json'

# --- DB接続設定 (環境変数から取得 - この部分は変更なし！) ---
# load_dotenv()のおかげで、os.environ.get()が.envの内容を読めるようになる
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_NAME = os.environ.get('DB_NAME', 'dog_travel_db')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

# --- テーブル情報 ---
TABLE_NAME = 'hotel_analysis_results'
# [修正] インデントを修正
COLUMNS = [
    'hotel_name', 'anshin_score_alltime', 'anshin_score_1year', 'total_reviews_alltime',
    'total_reviews_1year', 'sources', 'risk_points_alltime', 'risk_rate_alltime',
    'wow_points_alltime', 'wow_rate_alltime', 'risk_points_1year', 'risk_rate_1year',
    'wow_points_1year', 'wow_rate_1year'
]
JSON_KEYS = {
    'hotel_name': None, 'anshin_score_alltime': 'anshin_score_alltime', 'anshin_score_1year': 'anshin_score_1year',
    'total_reviews_alltime': 'total_reviews_alltime', 'total_reviews_1year': 'total_reviews_1year', 'sources': 'sources',
    'risk_points_alltime': ('risk_details_alltime', 'total_risk_points'), 'risk_rate_alltime': ('risk_details_alltime', 'risk_rate'),
    'wow_points_alltime': ('wow_details_alltime', 'total_wow_points'), 'wow_rate_alltime': ('wow_details_alltime', 'wow_rate'),
    'risk_points_1year': ('risk_details_1year', 'total_risk_points'), 'risk_rate_1year': ('risk_details_1year', 'risk_rate'),
    'wow_points_1year': ('wow_details_1year', 'total_wow_points'), 'wow_rate_1year': ('wow_details_1year', 'wow_rate')
}


def get_db_connection():
    """データベースへの接続を取得する"""
    if not DB_USER or not DB_PASSWORD:
        print("エラー: 環境変数 DB_USER または DB_PASSWORD が設定されていません。(環境変数または .env ファイルを確認してください)")
        sys.exit(1)

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print(f"データベース '{DB_NAME}' への接続に成功しました。")
        return conn
    except psycopg2.OperationalError as e:
        print(f"エラー: データベース接続に失敗しました。詳細: {e}")
        sys.exit(1)

def load_json_data(file_path):
    """JSONファイルからデータを読み込む"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            print(f"{file_path} から {len(data)}件のデータを読み込みました。")
            return data
    except FileNotFoundError:
        print(f"エラー: データファイル {file_path} が見つかりません。")
        return None
    except json.JSONDecodeError as e:
        print(f"エラー: {file_path} のJSON形式が不正です。詳細: {e}")
        return None

def upsert_data(conn, data):
    """データをDBにUPSERTする"""
    if not data:
        print("DBに書き込むデータがありません。")
        return 0

    cursor = None
    upserted_count = 0

    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(TABLE_NAME),
        sql.SQL(', ').join(map(sql.Identifier, COLUMNS)),
        sql.SQL(', ').join(sql.Placeholder() * len(COLUMNS))
    )
    update_columns = [col for col in COLUMNS if col != 'hotel_name']
    update_sql_part = sql.SQL(', ').join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
        for col in update_columns
    )
    upsert_sql = sql.SQL("{} ON CONFLICT (hotel_name) DO UPDATE SET {}, last_calculated_at = CURRENT_TIMESTAMP").format(
        insert_sql,
        update_sql_part
    )

    try:
        cursor = conn.cursor()
        print(f"{len(data)}件のデータをDBに書き込み開始...")

        for hotel_name, analysis_data in data.items():
            values = []
            valid_entry = True
            for col in COLUMNS:
                key_info = JSON_KEYS[col]
                value = None
                if key_info is None: value = hotel_name
                elif isinstance(key_info, tuple):
                    nested_dict = analysis_data.get(key_info[0], {})
                    value = nested_dict.get(key_info[1])
                else: value = analysis_data.get(key_info)

                if col == 'sources' and value is not None and not isinstance(value, list):
                     print(f"  [警告] {hotel_name}: 'sources' がリスト形式ではありません。スキップします。 Value: {value}")
                     valid_entry = False; break

                values.append(value)

            if valid_entry:
                try:
                    cursor.execute(upsert_sql, tuple(values))
                    upserted_count += 1
                except psycopg2.Error as db_err:
                     print(f"  [DBエラー] {hotel_name}: 書き込み中にエラー。スキップします。 詳細: {db_err}")
                     conn.rollback() # 個別エラーでもロールバックして次に進む
            # else: pass # スキップ処理

        conn.commit()
        print(f"-> {upserted_count}件のデータの書き込み（UPSERT）が完了しました。")
        return upserted_count

    except psycopg2.Error as e:
        print(f"エラー: データベース操作中にエラー。詳細: {e}")
        if conn: conn.rollback()
        return 0
    finally:
        if cursor: cursor.close()

def main():
    """メイン処理"""
    print("データベースローダーを起動します...")

    connection = get_db_connection()
    if not connection: return

    analysis_data = load_json_data(INPUT_JSON_FILE)

    processed_count = 0
    if analysis_data:
        processed_count = upsert_data(connection, analysis_data)

    if connection:
        connection.close()
        print("データベース接続を閉じました。")

    print(f"\n処理結果: {processed_count}件のホテルデータがDBに正常に書き込まれました。")

if __name__ == "__main__":
    main()

