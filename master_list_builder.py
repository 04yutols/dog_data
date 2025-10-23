import csv
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

# --- ★設定場所★ ---
# 収集したいじゃらんの検索結果URLをリストしたファイル名を指定
URL_LIST_FILE = 'search_urls_jalan.txt'
# -------------------------

# --- 設定項目 ---
OUTPUT_FILE = 'hotels_raw_jalan.csv' # 生成されるマスターリストのファイル名
REQUEST_DELAY = 1.0                  # 各リクエスト間の待機時間（秒）。サーバー負荷を考慮し、1秒を推奨。
REQUEST_TIMEOUT = 20                 # リクエストのタイムアウト時間（秒）

def main():
    """
    search_urls_jalan.txtから複数の起点URLを読み込み、
    全ての検索結果を巡回して、単一のマスターリストを生成する。
    """
    print("じゃらん用マスターリスト自動構築エンジン v9 (複数地域対応・最終版) を起動します...")
    
    # --- [変更] 複数の起点URLをファイルから読み込む ---
    try:
        with open(URL_LIST_FILE, 'r', encoding='utf-8') as f:
            search_base_urls = [line.strip() for line in f if line.strip()]
        if not search_base_urls:
            print(f"エラー: {URL_LIST_FILE} が空か、有効なURLがありません。")
            return
        print(f"{URL_LIST_FILE} から {len(search_base_urls)}件の起点URLを読み込みました。")
    except FileNotFoundError:
        print(f"エラー: {URL_LIST_FILE} が見つかりません。ファイルを作成してください。")
        return

    # [変更] 全てのホテルデータを一時的に格納するリストと、重複防止用のセット
    all_hotels_data = []
    unique_hotel_ids = set()

    # --- [変更] 読み込んだ起点URLごとにループ ---
    for i, base_url in enumerate(search_base_urls, 1):
        print("\n" + "="*50)
        print(f"[{i}/{len(search_base_urls)}] 起点URLの処理を開始: {base_url[:80]}...")
        print("="*50)
        
        page_count = 1
        while True:
            current_idx = (page_count - 1) * 30
            parsed_url = urlparse(base_url)
            query_params = parse_qs(parsed_url.query)
            query_params['idx'] = [str(current_idx)]
            new_query = urlencode(query_params, doseq=True)
            current_url = parsed_url._replace(query=new_query).geturl()
            
            print(f"\n[ {page_count}ページ目 ] (idx={current_idx}) を解析中...")

            try:
                headers = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5.0 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/5.36" }
                response = requests.get(current_url, headers=headers, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                if page_count == 1 and '件' not in soup.get_text():
                    print("  -> 1ページ目で文字化けを検知。CP932で再解析します。")
                    soup = BeautifulSoup(response.content, 'html.parser', from_encoding='CP932')

                hotel_items = soup.select('.p-yadoCassette.p-searchResultItem.js-searchResultItem')
                
                if not hotel_items:
                    print("  -> このページにホテル情報が見つかりませんでした。この起点URLの処理を終了します。")
                    break

                found_on_page = 0
                for item in hotel_items:
                    hotel_id = None
                    map_button = item.select_one('a.p-searchResultItem__mapButton')
                    if map_button and map_button.has_attr('onclick'):
                        try:
                            onclick_text = map_button['onclick']
                            hotel_id = onclick_text.split("yadNo=")[1].split("'")[0]
                        except IndexError: continue
                    
                    if not hotel_id: continue
                    if hotel_id in unique_hotel_ids: continue
                    
                    name_element = item.select_one('h2.p-searchResultItem__facilityName')
                    if name_element:
                        hotel_name = name_element.get_text(strip=True)
                        review_page_url = f"https://www.jalan.net/yad{hotel_id}/kuchikomi/"
                        
                        all_hotels_data.append({'hotel_name': hotel_name, 'url': review_page_url})
                        unique_hotel_ids.add(hotel_id)
                        found_on_page += 1
                
                print(f"  -> 新規に{found_on_page}件のホテル情報を抽出しました。")

                if found_on_page == 0 and page_count > 1:
                    print("  -> 新規のホテルが見つかりませんでした。最終ページと判断し、巡回を終了します。")
                    break
                
                page_count += 1

            except requests.exceptions.RequestException as e:
                print(f"\nエラー: ページの取得に失敗しました。この起点URLの処理をスキップします。 Error: {e}")
                break
            
            time.sleep(REQUEST_DELAY)

    # --- 収集した全データをCSVに書き出し ---
    if not all_hotels_data:
        print("\n1件もホテル情報を収集できませんでした。")
        return

    print("\n" + "="*50)
    print(f"全地域の収集が完了しました。合計 {len(all_hotels_data)}件のユニークなホテル情報を収集しました。")
    print(f"CSVファイル ({OUTPUT_FILE}) に書き出します...")

    try:
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=['hotel_name', 'url'])
            writer.writeheader()
            writer.writerows(all_hotels_data)
        print("じゃらん用マスターリストの構築が完了しました！")
    except IOError as e:
        print(f"エラー: ファイルの書き込みに失敗しました。 Error: {e}")
    
    print("="*50)

if __name__ == "__main__":
    main()

