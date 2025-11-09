import json
import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import socket # [追加] ホスト名をIPアドレスに解決するため

# .env ファイルの読み込み処理 (変更なし)
script_dir = os.path.dirname(os.path.abspath(__file__))
# ... (dotenv_path, loaded の定義は変更なし) ...
project_root = os.path.dirname(script_dir)
dotenv_path = os.path.join(project_root, '.env')
loaded = load_dotenv(dotenv_path=dotenv_path, override=True)
if loaded:
    print(f".env ファイル ({dotenv_path}) の読み込みに成功しました。")
else:
    print(f"警告: .env ファイル ({dotenv_path}) が見つからないか、読み込めませんでした。")

# --- ファイル設定 (変更なし) ---
INPUT_JSON_FILE = os.path.join(project_root, 'data/output/analysis_results.json')

# --- DB接続設定 (変更なし) ---
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_PORT = os.environ.get('DB_PORT', '6543')

# --- テーブル情報 (変更なし) ---
TABLE_NAME = 'hotel_analysis_results'
# ... (COLUMNS, JSON_KEYS の定義は変更なし) ...
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
    """データベースへの接続を取得する (Supabase IPv4 Fix v2)"""
    print(f"[DEBUG] Attempting connection with:")
    print(f"[DEBUG]   DB_HOST (Original): '{DB_HOST}'")
    print(f"[DEBUG]   DB_NAME: '{DB_NAME}'")
    # ... (他のデバッグログ) ...
    print(f"[DEBUG]   DB_PORT: '{DB_PORT}'")
    print(f"[DEBUG]   DB_USER: '{DB_USER}'")
    print(f"[DEBUG]   DB_PASSWORD: {'******' if DB_PASSWORD else 'None'}")
    
    if not DB_HOST or not DB_NAME or not DB_USER or not DB_PASSWORD:
        print("エラー: DB接続に必要な環境変数 (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD) が不足しています。")
        sys.exit(1)

    # --- [変更] IPv4 アドレスへの強制解決を、より堅牢な getaddrinfo に変更 ---
    resolved_host_ip = DB_HOST # デフォルトは元のホスト名
    try:
        # socket.AF_INET は IPv4 のみを強制的に探させる
        addr_info = socket.getaddrinfo(DB_HOST, DB_PORT, family=socket.AF_INET)
        # 解決したアドレスリストから最初のIPv4アドレスを取得
        if addr_info:
            resolved_host_ip = addr_info[0][4][0]
            print(f"[DEBUG] ホスト名 '{DB_HOST}' を IPv4 アドレス '{resolved_host_ip}' に強制解決しました。")
    except socket.gaierror as e:
        print(f"警告: ホスト名 '{DB_HOST}' のIPv4アドレス解決に失敗。エラー: {e}")
        print("       元のホスト名で接続を試みますが、IPv6問題が再発する可能性があります。")
        # 解決に失敗した場合は、`resolved_host_ip` は元の `DB_HOST` のまま
        pass
    # -----------------------------------------------------------------

    try:
        conn = psycopg2.connect(
            host=resolved_host_ip, # [変更] 解決したIPv4アドレス (または元のホスト名) を使用
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            sslmode='require' # SSL接続を強制
        )
        print(f"データベース '{DB_NAME}' (Supabase) への接続に成功しました。")
        return conn
    except psycopg2.OperationalError as e:
        print(f"エラー: データベース接続に失敗しました。詳細: {e}")
        print("ヒント: IPv6/IPv4の接続問題か、Supabaseのネットワーク制限を再確認してください。")
        sys.exit(1)

# --- load_json_data 関数 (変更なし) ---
def load_json_data(file_path):
    # ... (関数の中身は変更なし) ...
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

# --- upsert_data 関数 (変更なし) ---
def upsert_data(conn, data):
    # ... (関数の中身は変更なし) ...
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
                     conn.rollback() 
            
        conn.commit()
        print(f"-> {upserted_count}件のデータの書き込み（UPSERT）が完了しました。")
        return upserted_count
    except psycopg2.Error as e:
        print(f"エラー: データベース操作中にエラー。詳細: {e}")
        if conn: conn.rollback()
        return 0
    finally:
        if cursor: cursor.close()

# --- main 関数 (変更なし) ---
def main():
    """メイン処理"""
    print("データベースローダー (Supabase IPv4 Fix v2) を起動します...")
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