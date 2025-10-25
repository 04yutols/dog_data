import csv
import json
import time
import re # ホテルID抽出のために正規表現ライブラリをインポート
from datetime import datetime, timedelta
from multiprocessing import Pool, Manager, freeze_support # freeze_supportを追加
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import requests
from bs4 import BeautifulSoup

# --- [NEW] 日付解析用ライブラリ ---
# もし入ってなければ pip install python-dateutil
from dateutil.parser import parse as date_parse
import locale

# --- 設定項目 ---
RAKUTEN_MASTER_FILE = 'hotels_raw.csv'
JALAN_MASTER_FILE = 'hotels_raw_jalan.csv'
DATA_FILE = 'hotel_data.json'        # レビューを保存/追記するデータファイル
OUTPUT_FILE = 'hotel_data.json'      # 出力ファイル名

# --- パフォーマンス & 安全性設定 ---
MAX_WORKERS = 4                      # 同時に動かす分身の数
REQUESTS_PER_SECOND = 2              # 1秒あたりの最大リクエスト数
REFRESH_DAYS = 30                    # この日数より古いデータは再取得の対象とする

# --- ロケール設定 (日本語日付解析のため) ---
try:
    locale.setlocale(locale.LC_TIME, 'ja_JP.UTF-8')
except locale.Error:
    print("警告: 日本語ロケール'ja_JP.UTF-8'の設定に失敗。日付解析注意。")
    try: locale.setlocale(locale.LC_TIME, 'Japanese_Japan.932')
    except locale.Error: print("警告: 代替ロケールも失敗。")


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
        if match: return f"rakuten_{match.group(1)}"
    elif "jalan.net/yad" in url:
        # /kuchikomi/ と /kuchikomi/archive/ の両方に対応
        match = re.search(r'/yad(\d+)/kuchikomi(?:/archive)?', url)
        if match: return f"jalan_{match.group(1)}"
    return None

def load_target_hotels(rakuten_file, jalan_file):
    """楽天とじゃらんのCSVを読み込み、ユニークIDをキーとした辞書を生成する"""
    targets = {}
    files_to_load = {rakuten_file: "rakuten", jalan_file: "jalan"}

    for file_path, source in files_to_load.items():
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f: # utf-8-sigでBOMを処理
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get('url')
                    name = row.get('hotel_name')
                    if not url or not name: continue
                    unique_id = generate_unique_id(url)
                    if unique_id and unique_id not in targets:
                         targets[unique_id] = {'hotel_name': name, 'url': url, 'source': source}
        except FileNotFoundError:
            print(f"警告: {file_path} が見つかりません。")
        except Exception as e:
            print(f"エラー: {file_path} の読み込み中にエラー: {e}")

    return targets

def determine_scrape_targets(targets, existing_data):
    """
    ユニークIDを基準に、差分と鮮度、レビュー形式をチェックし、更新対象のリストを返す。
    """
    print("更新対象のホテルを抽出中...")
    todo_list = {}
    thirty_days_ago = datetime.now() - timedelta(days=REFRESH_DAYS)

    for unique_id, data in targets.items():
        needs_update = False
        if unique_id not in existing_data:
            needs_update = True # 完全新規
            print(f"  -> {data['hotel_name']} ({data['source']}): 新規のため更新対象")
        else:
            hotel_entry = existing_data[unique_id]
            last_updated_str = hotel_entry.get('last_updated')
            reviews_data = hotel_entry.get('reviews', [])
            # [修正] レビュー形式が古いか、日付がNoneかチェック
            is_old_format = any(
                isinstance(r, str) or
                (isinstance(r, dict) and r.get('date') is None and data['source'] == 'jalan') or # Jalanで日付がNoneなら更新対象
                (isinstance(r, dict) and 'date' not in r) # dateキー自体がない
                for r in reviews_data
            )

            if is_old_format: # 古い形式なら更新
                 needs_update = True
                 print(f"  -> {data['hotel_name']} ({data['source']}): 古いレビュー形式/日付未取得のため更新対象")
            elif not last_updated_str:
                 needs_update = True
                 print(f"  -> {data['hotel_name']} ({data['source']}): 最終更新日不明のため更新対象")
            else:
                try:
                    last_updated = datetime.fromisoformat(last_updated_str)
                    if last_updated < thirty_days_ago:
                        needs_update = True
                        print(f"  -> {data['hotel_name']} ({data['source']}): データが古いため更新対象 (最終更新: {last_updated_str})")
                except ValueError:
                     needs_update = True
                     print(f"  -> {data['hotel_name']} ({data['source']}): 不正な最終更新日のため更新対象")

        if needs_update:
            todo_list[unique_id] = data

    print(f"-> {len(todo_list)}件のホテルが更新対象です。")
    return todo_list

def parse_review_date(date_str, source):
    """日付文字列を "YYYY-MM-DD" に変換 (ソースに応じて処理)"""
    if not date_str: return None

    try:
        # まず共通のプレフィックスを除去
        cleaned_date = re.sub(r'^(投稿日：|投稿日:|【利用時期】)\s*', '', date_str.strip())

        if source == "rakuten":
            # 楽天 ("YYYY年MM月DD日 HH:MM:SS" 形式を優先)
            try:
                dt = datetime.strptime(cleaned_date, '%Y年%m月%d日 %H:%M:%S')
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                dt = date_parse(cleaned_date) # だめなら dateutil に任せる
                return dt.strftime('%Y-%m-%d')
        elif source == "jalan":
             # じゃらん ("YYYY/MM/DD" 形式を優先)
             try:
                 dt = datetime.strptime(cleaned_date, '%Y/%m/%d')
                 return dt.strftime('%Y-%m-%d')
             except ValueError:
                 dt = date_parse(cleaned_date) # だめなら dateutil に任せる
                 return dt.strftime('%Y-%m-%d')
        else:
             return None
    except (ValueError, TypeError):
        print(f"  [警告] 解析不能な日付形式 ({source}): '{date_str}'")
        return None

def scrape_hotel_reviews_worker(args):
    """
    【現場作業員】1軒のホテルの全レビュー（日付付き）を取得する。
    """
    unique_id, data, rate_limiter = args
    name = data['hotel_name']
    url = data['url']
    source = data['source']

    with rate_limiter['lock']:
        elapsed = time.monotonic() - rate_limiter['last_call']
        wait_time = (1.0 / REQUESTS_PER_SECOND) - elapsed
        if wait_time > 0: time.sleep(wait_time)
        rate_limiter['last_call'] = time.monotonic()

    print(f"  [作業開始] {name} ({source})")

    reviews_with_dates = []
    page_num = 1
    page_offset = 0
    last_page_first_review_text = None

    while True:
        try:
            headers = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36" }
            current_page_url = ""

            # --- ソース別ページネーション (変更なし) ---
            if source == "rakuten":
                parsed_url = urlparse(url); query_params = parse_qs(parsed_url.query)
                query_params['f_next'] = [str(page_offset)]
                new_query = urlencode(query_params, doseq=True)
                current_page_url = parsed_url._replace(query=new_query).geturl()
            elif source == "jalan":
                if page_num == 1: current_page_url = url
                else:
                    parsed_url = urlparse(url); path = parsed_url.path.rstrip('/')
                    base_kuchikomi_path = path.rsplit('/', 1)[0] if '/archive' in path else path
                    if not base_kuchikomi_path.endswith('kuchikomi'): # 念のため
                         base_kuchikomi_path += '/kuchikomi'
                    
                    new_path_segment = f"{page_num}.HTML"
                    # アーカイブパスを維持
                    if '/archive' in path:
                         new_path = f"{base_kuchikomi_path}/archive/{new_path_segment}"
                    else:
                         new_path = f"{base_kuchikomi_path}/{new_path_segment}"

                    current_page_url = urlunparse(parsed_url._replace(path=new_path))
            else: return unique_id, data, None, "不明なソース"

            response = requests.get(current_page_url, headers=headers, timeout=20)
            if source == "jalan" and response.status_code == 404: break
            response.raise_for_status()

            # --- 文字コード処理 (変更なし) ---
            encoding_to_use = None
            if source == 'jalan' and page_num == 1:
                 temp_soup = BeautifulSoup(response.content, 'html.parser')
                 if '件' not in temp_soup.get_text(): encoding_to_use = 'CP932'
            soup = BeautifulSoup(response.content, 'html.parser', from_encoding=encoding_to_use)

            # --- [変更] レビュー抽出 (じゃらんの日付取得を正確に実装) ---
            review_elements_found_on_page = False
            if source == "rakuten":
                review_blocks = soup.select('dl.commentReputation')
                if not review_blocks: break
                for block in review_blocks:
                    date_element = block.select_one('dt > span.time')
                    text_element = block.select_one('dd > p.commentSentence')
                    if date_element and text_element:
                        raw_date_str = date_element.get_text(strip=True)
                        review_text = text_element.get_text(strip=True)
                        formatted_date = parse_review_date(raw_date_str, source)
                        if review_text:
                            reviews_with_dates.append({"date": formatted_date, "text": review_text})
                            review_elements_found_on_page = True

            elif source == "jalan":
                 # HTMLスニペットに基づいてセレクタを正確に指定
                 review_blocks = soup.select('div.jlnpc-kuchikomiCassette__contWrap')
                 if not review_blocks: break

                 # Jalan無限ループ防止のための準備
                 if review_blocks:
                      current_first_text_el = review_blocks[0].select_one('p.jlnpc-kuchikomiCassette__postBody')
                      current_first = current_first_text_el.get_text(strip=True) if current_first_text_el else None
                      # 最初のページ以外で、かつ最初のレビューが前回と同じならループ終了
                      if page_num > 1 and current_first == last_page_first_review_text:
                           print("-> 前のページと同じ内容を検出しました。このセクションの取得を完了します。")
                           break
                      last_page_first_review_text = current_first

                 for block in review_blocks:
                     # 日付要素: div.jlnpc-kuchikomiCassette__rightArea p.jlnpc-kuchikomiCassette__postDate
                     date_element = block.select_one('div.jlnpc-kuchikomiCassette__rightArea p.jlnpc-kuchikomiCassette__postDate')
                     # 本文要素: div.jlnpc-kuchikomiCassette__rightArea p.jlnpc-kuchikomiCassette__postBody
                     text_element = block.select_one('div.jlnpc-kuchikomiCassette__rightArea p.jlnpc-kuchikomiCassette__postBody')

                     if text_element: # 本文があれば処理
                         raw_date_str = date_element.get_text(strip=True) if date_element else None
                         review_text = text_element.get_text(strip=True)
                         
                         formatted_date = parse_review_date(raw_date_str, source)

                         if review_text:
                             reviews_with_dates.append({"date": formatted_date, "text": review_text})
                             review_elements_found_on_page = True

            if not review_elements_found_on_page: break

            # 次ページへ
            if source == "rakuten": page_offset += 20
            elif source == "jalan": page_num += 1

            time.sleep(0.5)

        except requests.RequestException as e:
            return unique_id, data, None, str(e)
        except Exception as e_gen:
             return unique_id, data, None, f"予期せぬエラー: {e_gen}"

    return unique_id, data, reviews_with_dates, None # 日付付きリストを返す

def main():
    """
    【司令塔】楽天とじゃらんのデータを統合し、並列処理でレビューを取得する。
    """
    existing_data = load_existing_data(DATA_FILE)
    target_hotels = load_target_hotels(RAKUTEN_MASTER_FILE, JALAN_MASTER_FILE)

    if not target_hotels: return

    todo_hotels = determine_scrape_targets(target_hotels, existing_data)

    if not todo_hotels:
        print("更新対象のホテルはありません。処理を終了します。")
        return

    manager = Manager()
    rate_limiter = manager.dict({'lock': manager.Lock(), 'last_call': time.monotonic()})
    tasks = [(uid, data, rate_limiter) for uid, data in todo_hotels.items()]

    print(f"\n{MAX_WORKERS}並列でスクレイピングを開始します ({len(tasks)}件)...")

    # [追加] Windows環境でのmultiprocessing問題を回避するためのおまじない
    freeze_support()

    with Pool(processes=MAX_WORKERS) as pool:
        results = pool.map(scrape_hotel_reviews_worker, tasks)

    print("\n全ワーカーの処理が完了。結果を集約します...")

    success_count = 0
    no_review_count = 0
    error_count = 0
    for unique_id, data, reviews_with_dates, error in results:
        if error:
            print(f"  [エラー] {data['hotel_name']} ({data['source']}): {error}")
            error_count += 1
        elif reviews_with_dates and all(isinstance(r, dict) for r in reviews_with_dates):
            existing_data[unique_id] = {
                'hotel_name': data['hotel_name'],
                'url': data['url'],
                'source': data['source'],
                'reviews': reviews_with_dates, # 日付付きリストを保存
                'last_updated': datetime.now().isoformat()
            }
            success_count += 1
        else:
            print(f"  [警告] {data['hotel_name']} ({data['source']}): レビューが見つからずスキップ。")
            no_review_count += 1

    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)
        print("\n" + "="*40); print("処理完了。")
        print(f"  - 成功 (データ更新): {success_count}件")
        print(f"  - 警告 (レビュー無し): {no_review_count}件")
        print(f"  - エラー: {error_count}件")
        print(f"最新データが {OUTPUT_FILE} に保存されました。"); print("="*40)
    except IOError as e:
        print(f"エラー: ファイルの書き込みに失敗しました。 {e}")

if __name__ == '__main__':
    main()

