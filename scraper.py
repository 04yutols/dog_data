import csv
import json
import time
from datetime import datetime, timedelta
from multiprocessing import Pool, Manager
from urllib.parse import urlparse, parse_qs, urlencode
import requests
from bs4 import BeautifulSoup

# --- 設定項目 ---
MASTER_LIST_FILE = 'hotels_raw.csv'  # master_list_builder.pyが生成した全ホテルリスト
DATA_FILE = 'hotel_data.json'        # レビューを保存/追記するデータファイル
OUTPUT_FILE = 'hotel_data.json'      # 出力ファイル名（DATA_FILEと同じでOK）

# --- パフォーマンス & 安全性設定 ---
MAX_WORKERS = 4                      # 同時に動かす分身の数（CPUコア数に合わせると良い）
REQUESTS_PER_SECOND = 2              # 1秒あたりの最大リクエスト数（サーバーへの配慮）
REFRESH_DAYS = 30                    # この日数より古いデータは再取得の対象とする

def load_existing_data(file_path):
    """既存のレビューデータ(hotel_data.json)を読み込む"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def load_target_urls(file_path):
    """処理対象の全ホテルURL(hotels_raw.csv)を読み込む"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return {row['hotel_name']: row['url'] for row in reader}
    except FileNotFoundError:
        return {}

def determine_scrape_targets(targets, existing_data):
    """
    差分と鮮度をチェックし、今回本当にスクレイピングすべきURLのリストを返す。
    """
    print("更新対象のホテルを抽出中...")
    todo_list = {}
    thirty_days_ago = datetime.now() - timedelta(days=REFRESH_DAYS)

    for name, url in targets.items():
        if url not in existing_data:
            # 1. 完全新規のホテル
            todo_list[name] = url
        else:
            # 2. 既存だが、データが古いホテル
            last_updated_str = existing_data[url].get('last_updated')
            if last_updated_str:
                last_updated = datetime.fromisoformat(last_updated_str)
                if last_updated < thirty_days_ago:
                    todo_list[name] = url
            else: # last_updatedがない古いデータ形式の場合も更新対象
                todo_list[name] = url

    print(f"-> {len(todo_list)}件のホテルが更新対象です。")
    return todo_list

def scrape_hotel_reviews(args):
    """
    【現場作業員】1軒のホテルの全レビューを取得する。並列処理で呼ばれる関数。
    """
    name, url, rate_limiter = args
    
    # レートリミッター（交通整理員）に許可をもらう
    with rate_limiter['lock']:
        elapsed = time.monotonic() - rate_limiter['last_call']
        wait_time = (1.0 / REQUESTS_PER_SECOND) - elapsed
        if wait_time > 0:
            time.sleep(wait_time)
        rate_limiter['last_call'] = time.monotonic()

    print(f"  [作業開始] {name}")
    
    reviews = []
    page_offset = 0
    while True:
        try:
            # URLのf_nextパラメータを書き換えてページを巡回
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            query_params['f_next'] = [str(page_offset)]
            new_query = urlencode(query_params, doseq=True)
            current_page_url = parsed_url._replace(query=new_query).geturl()

            response = requests.get(current_page_url, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            review_elements = soup.select(".commentSentence")
            if not review_elements:
                break # レビューがなくなったら終了

            for rev in review_elements:
                reviews.append(rev.get_text(strip=True))

            page_offset += 20
            time.sleep(0.5) # ページネーション間の小休止

        except requests.RequestException as e:
            return name, url, None, str(e) # エラーを返す
            
    return name, url, reviews, None # 成功

def main():
    """
    【司令塔】全体の処理を統括する。
    """
    existing_data = load_existing_data(DATA_FILE)
    target_hotels = load_target_urls(MASTER_LIST_FILE)

    if not target_hotels:
        print(f"エラー: {MASTER_LIST_FILE}に処理対象がありません。")
        return

    todo_hotels = determine_scrape_targets(target_hotels, existing_data)

    if not todo_hotels:
        print("更新対象のホテルはありません。処理を終了します。")
        return
    
    # --- 並列処理 & レートリミットの準備 ---
    manager = Manager()
    rate_limiter = manager.dict({
        'lock': manager.Lock(),
        'last_call': time.monotonic()
    })
    
    # ワーカーに渡す引数のリストを作成
    tasks = [(name, url, rate_limiter) for name, url in todo_hotels.items()]

    print(f"\n{MAX_WORKERS}並列でスクレイピングを開始します...")
    
    # --- 並列処理の実行 ---
    with Pool(processes=MAX_WORKERS) as pool:
        results = pool.map(scrape_hotel_reviews, tasks)

    print("\n全ワーカーの処理が完了。結果を集約します...")
    
    # --- 結果を既存データにマージ ---
    success_count = 0
    for name, url, reviews, error in results:
        if error:
            print(f"  [エラー] {name}: {error}")
        else:
            existing_data[url] = {
                'hotel_name': name,
                'reviews': reviews,
                'last_updated': datetime.now().isoformat()
            }
            success_count += 1
    
    # --- 最終結果をファイルに保存 ---
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)
        print("\n" + "="*40)
        print(f"処理完了。{success_count}件のホテルデータを更新しました。")
        print(f"最新データが {OUTPUT_FILE} に保存されました。")
        print("="*40)
    except IOError as e:
        print(f"エラー: ファイルの書き込みに失敗しました。 {e}")

if __name__ == '__main__':
    main()
