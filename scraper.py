import csv
import json
import time
import re # ホテルID抽出のために正規表現ライブラリをインポート
from datetime import datetime, timedelta
from multiprocessing import Pool, Manager
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import requests
from bs4 import BeautifulSoup

# --- 設定項目 ---
# [変更] 楽天とじゃらん、両方のマスターリストファイルを指定
RAKUTEN_MASTER_FILE = 'hotels_raw.csv'
JALAN_MASTER_FILE = 'hotels_raw_jalan.csv'
DATA_FILE = 'hotel_data.json'        # レビューを保存/追記するデータファイル
OUTPUT_FILE = 'hotel_data.json'      # 出力ファイル名

# --- パフォーマンス & 安全性設定 ---
MAX_WORKERS = 4                      # 同時に動かす分身の数
REQUESTS_PER_SECOND = 2              # 1秒あたりの最大リクエスト数
REFRESH_DAYS = 30                    # この日数より古いデータは再取得の対象とする

def load_existing_data(file_path):
    """既存のレビューデータ(hotel_data.json)を読み込む"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def generate_unique_id(url):
    """URLからソース名とホテルIDを抽出し、ユニークIDを生成する"""
    if "review.travel.rakuten.co.jp" in url:
        match = re.search(r'/hotel/voice/(\d+)/', url)
        if match:
            return f"rakuten_{match.group(1)}"
    elif "jalan.net/yad" in url:
        match = re.search(r'/yad(\d+)/kuchikomi/', url)
        if match:
            return f"jalan_{match.group(1)}"
    return None # 不明なURL形式

def load_target_hotels(rakuten_file, jalan_file):
    """楽天とじゃらんのCSVを読み込み、ユニークIDをキーとした辞書を生成する"""
    targets = {}
    files_to_load = {rakuten_file: "rakuten", jalan_file: "jalan"}
    
    for file_path, source in files_to_load.items():
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f: # utf-8-sigでBOMを処理
                reader = csv.DictReader(f)
                for row in reader:
                    url = row['url']
                    name = row['hotel_name']
                    unique_id = generate_unique_id(url)
                    if unique_id and unique_id not in targets: # 重複を避ける
                         targets[unique_id] = {'hotel_name': name, 'url': url, 'source': source}
        except FileNotFoundError:
            print(f"警告: {file_path} が見つかりません。")
        except Exception as e:
            print(f"エラー: {file_path} の読み込み中にエラーが発生しました: {e}")
            
    return targets

def determine_scrape_targets(targets, existing_data):
    """
    ユニークIDを基準に、差分と鮮度をチェックし、更新対象のリストを返す。
    """
    print("更新対象のホテルを抽出中...")
    todo_list = {}
    thirty_days_ago = datetime.now() - timedelta(days=REFRESH_DAYS)

    for unique_id, data in targets.items():
        if unique_id not in existing_data:
            todo_list[unique_id] = data # 完全新規
        else:
            last_updated_str = existing_data[unique_id].get('last_updated')
            try:
                if last_updated_str:
                    last_updated = datetime.fromisoformat(last_updated_str)
                    if last_updated < thirty_days_ago:
                        todo_list[unique_id] = data # データが古い
                else:
                    todo_list[unique_id] = data # last_updatedがない
            except ValueError: # 不正な日付フォーマットの場合も更新対象
                 todo_list[unique_id] = data

    print(f"-> {len(todo_list)}件のホテルが更新対象です。")
    return todo_list

def scrape_hotel_reviews_worker(args):
    """
    【現場作業員】1軒のホテルの全レビューを取得する。ソースに応じて処理を分岐。
    """
    unique_id, data, rate_limiter = args
    name = data['hotel_name']
    url = data['url']
    source = data['source']
    
    # レートリミット
    with rate_limiter['lock']:
        elapsed = time.monotonic() - rate_limiter['last_call']
        wait_time = (1.0 / REQUESTS_PER_SECOND) - elapsed
        if wait_time > 0: time.sleep(wait_time)
        rate_limiter['last_call'] = time.monotonic()

    print(f"  [作業開始] {name} ({source})")
    
    reviews = []
    page_num = 1 # Jalan用ページ番号
    page_offset = 0 # Rakuten用オフセット
    last_page_first_review_text = None # Jalan用無限ループ防止
    
    while True:
        try:
            headers = { "User-Agent": "Mozilla/5.0..." } # User-Agent省略
            current_page_url = ""

            # --- ソース別ページネーション ---
            if source == "rakuten":
                parsed_url = urlparse(url)
                query_params = parse_qs(parsed_url.query)
                query_params['f_next'] = [str(page_offset)]
                new_query = urlencode(query_params, doseq=True)
                current_page_url = parsed_url._replace(query=new_query).geturl()
                
            elif source == "jalan":
                if page_num == 1:
                    current_page_url = url
                else:
                    parsed_url = urlparse(url)
                    path = parsed_url.path.rstrip('/')
                    if f"{page_num-1}.HTML" in path:
                         new_path = path.replace(f"{page_num-1}.HTML", f"{page_num}.HTML")
                    else:
                         new_path = f"{path}/{page_num}.HTML"
                    current_page_url = urlunparse(parsed_url._replace(path=new_path))
            else:
                 return unique_id, data, None, "不明なソース"
            
            # --- HTML取得と解析 ---
            response = requests.get(current_page_url, headers=headers, timeout=20)
            if source == "jalan" and response.status_code == 404: break # Jalan 404は終端
            response.raise_for_status()

            # 文字コード処理 (Jalan 1ページ目のみ特殊対応)
            encoding_to_use = None
            if source == 'jalan' and page_num == 1:
                 # まず自動判別を試す
                 temp_soup = BeautifulSoup(response.content, 'html.parser')
                 if '件' not in temp_soup.get_text(): # 文字化け兆候あり
                      encoding_to_use = 'CP932' # CP932を試す

            # 指定されたエンコーディング、または自動判別で解析
            soup = BeautifulSoup(response.content, 'html.parser', from_encoding=encoding_to_use)

            # --- レビュー抽出 (ソース別セレクタ) ---
            review_elements = []
            if source == "rakuten":
                review_elements = soup.select(".commentSentence")
            elif source == "jalan":
                 review_elements = soup.select('p.jlnpc-kuchikomiCassette__postBody')

            if not review_elements: break # レビューが見つからなければ終了

            # Jalan無限ループ防止
            if source == "jalan":
                current_first = review_elements[0].get_text(strip=True)
                if current_first == last_page_first_review_text: break
                last_page_first_review_text = current_first

            for rev in review_elements: reviews.append(rev.get_text(strip=True))

            # 次ページへ
            if source == "rakuten": page_offset += 20
            elif source == "jalan": page_num += 1
            
            time.sleep(0.5) # ページネーション間隔

        except requests.RequestException as e:
            return unique_id, data, None, str(e) # エラー
        except Exception as e_gen: # 予期せぬエラー
             return unique_id, data, None, f"予期せぬエラー: {e_gen}"
            
    return unique_id, data, reviews, None # 成功

def main():
    """
    【司令塔】楽天とじゃらんのデータを統合し、並列処理でレビューを取得する。
    """
    existing_data = load_existing_data(DATA_FILE)
    target_hotels = load_target_hotels(RAKUTEN_MASTER_FILE, JALAN_MASTER_FILE)

    if not target_hotels:
        print(f"エラー: 処理対象のホテルリストが読み込めません。")
        return

    todo_hotels = determine_scrape_targets(target_hotels, existing_data)

    if not todo_hotels:
        print("更新対象のホテルはありません。処理を終了します。")
        return
    
    manager = Manager()
    rate_limiter = manager.dict({'lock': manager.Lock(), 'last_call': time.monotonic()})
    
    # ワーカーに渡す引数のリスト (unique_id, data_dict, rate_limiter)
    tasks = [(uid, data, rate_limiter) for uid, data in todo_hotels.items()]

    print(f"\n{MAX_WORKERS}並列でスクレイピングを開始します ({len(tasks)}件)...")
    
    with Pool(processes=MAX_WORKERS) as pool:
        results = pool.map(scrape_hotel_reviews_worker, tasks)

    print("\n全ワーカーの処理が完了。結果を集約します...")
    
    success_count = 0
    no_review_count = 0
    error_count = 0
    for unique_id, data, reviews, error in results:
        if error:
            print(f"  [エラー] {data['hotel_name']} ({data['source']}): {error}")
            error_count += 1
        elif reviews:
            # 新しいデータ構造で保存
            existing_data[unique_id] = {
                'hotel_name': data['hotel_name'],
                'url': data['url'],
                'source': data['source'],
                'reviews': reviews,
                'last_updated': datetime.now().isoformat()
            }
            success_count += 1
        else:
            print(f"  [警告] {data['hotel_name']} ({data['source']}): レビューが見つからずスキップ。")
            no_review_count += 1
    
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)
        print("\n" + "="*40)
        print("処理完了。")
        print(f"  - 成功 (データ更新): {success_count}件")
        print(f"  - 警告 (レビュー無し): {no_review_count}件")
        print(f"  - エラー: {error_count}件")
        print(f"最新データが {OUTPUT_FILE} に保存されました。")
        print("="*40)
    except IOError as e:
        print(f"エラー: ファイルの書き込みに失敗しました。 {e}")

# [変更] シンプルに main() を呼び出す形にする
if __name__ == '__main__':
    main()

