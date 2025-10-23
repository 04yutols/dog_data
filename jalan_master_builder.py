import csv
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

# --- ★書き換える場所★ ---
# じゃらんで「犬と泊まる宿」などを検索した【1ページ目】のURLを貼り付けてください
SEARCH_BASE_URL = "https://www.jalan.net/theme/pet/tochigi/?roomCount=1&ssc=57165&adultNum=2&distCd=05&stayYear=&stayMonth=&dateUndecided=1&roomCrack=200000&vosFlg=6&mvTabFlg=1&stayDay=&screenId=UWW1402&activeSort=0&stayCount=1&rootCd=0157165"
# -------------------------

# --- 設定項目 ---
OUTPUT_FILE = 'hotels_raw_jalan.csv' # 生成されるマスターリストのファイル名
REQUEST_DELAY = 1.0                  # 各リクエスト間の待機時間（秒）。サーバー負荷を考慮し、1秒を推奨。
REQUEST_TIMEOUT = 20                 # リクエストのタイムアウト時間（秒）

def main():
    """
    じゃらんの検索結果を全ページ巡回し、ホテル名とレビューページのURLリストを生成する。
    1ページ目の文字化けを複数のエンコーディングで再試行する「尋問官」モデル。
    """
    if not SEARCH_BASE_URL or SEARCH_BASE_URL == "（ここにじゃらんの検索結果1ページ目のURLを貼る）":
        print("エラー: 検索URLが設定されていません。SEARCH_BASE_URLを書き換えてください。")
        return

    print("じゃらん用マスターリスト自動構築エンジン v8 (尋問官モデル) を起動します...")
    print(f"起点URL: {SEARCH_BASE_URL}")

    all_hotels_data = []
    unique_hotel_ids = set() # 重複するホテルを防止するためのセット
    page_count = 1

    while True:
        
        current_idx = (page_count - 1) * 30
        
        parsed_url = urlparse(SEARCH_BASE_URL)
        query_params = parse_qs(parsed_url.query)
        query_params['idx'] = [str(current_idx)] # idxパラメータを上書き
        
        new_query = urlencode(query_params, doseq=True)
        current_url = parsed_url._replace(query=new_query).geturl()
        
        print(f"\n[ {page_count}ページ目 ] (idx={current_idx}) を解析中...")
        print(f"URL: {current_url}")

        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5.0 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/5.36"
            }
            response = requests.get(current_url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            # --- [変更] ここが新しい心臓部「尋問官」 ---
            # まずはBeautifulSoupの自動判別に任せる
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 1ページ目だけで、かつ文字化けの兆候（日本語特有の「件」がない）が見られる場合のみ...
            if page_count == 1 and '件' not in soup.get_text():
                print("  -> 1ページ目で文字化けを検知。エンコーディングの尋問を開始します...")
                
                # 「尋問」リスト：怪しいエンコーディングを順番に試す
                encodings_to_try = ['CP932', 'Shift_JIS', 'EUC-JP']
                
                for encoding in encodings_to_try:
                    print(f"    -> 容疑者: {encoding} で再解析を試行...")
                    # ...指定したエンコーディングで強制的に再解析
                    temp_soup = BeautifulSoup(response.content, 'html.parser', from_encoding=encoding)
                    
                    # 再解析した結果、正しい日本語（「件」）が見つかったか？
                    if '件' in temp_soup.get_text():
                        print(f"    -> 自白！ 正しいエンコーディングは {encoding} と断定。")
                        soup = temp_soup # 正解のsoupを採用
                        break # 尋問終了
                else: # for-else: ループがbreakされずに終わった場合
                    print("    -> 全ての容疑者がシラを切り通しました。このページの解析を断念します。")
            # ---------------------------------------------

            hotel_items = soup.select('.p-yadoCassette.p-searchResultItem.js-searchResultItem')
            
            if not hotel_items:
                print("  -> このページにホテル情報が見つかりませんでした。巡回を終了します。")
                break

            found_on_page = 0
            for item in hotel_items:
                hotel_id = None
                map_button = item.select_one('a.p-searchResultItem__mapButton')
                if map_button and map_button.has_attr('onclick'):
                    try:
                        onclick_text = map_button['onclick']
                        hotel_id = onclick_text.split("yadNo=")[1].split("'")[0]
                    except IndexError:
                        continue
                
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
            print(f"\nエラー: ページの取得に失敗しました。URL: {current_url}, Error: {e}")
            break
        
        time.sleep(REQUEST_DELAY)

    # --- 収集した全データをCSVに書き出し ---
    if not all_hotels_data:
        print("\n1件もホテル情報を収集できませんでした。")
        return

    print("\n" + "="*40)
    print(f"合計 {len(all_hotels_data)}件のユニークなホテル情報を収集しました。")
    print(f"CSVファイル ({OUTPUT_FILE}) に書き出します...")

    try:
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=['hotel_name', 'url'])
            writer.writeheader()
            writer.writerows(all_hotels_data)
        print("じゃらん用マスターリストの構築が完了しました！")
    except IOError as e:
        print(f"エラー: ファイルの書き込みに失敗しました。 Error: {e}")
    
    print("="*40)

if __name__ == "__main__":
    main()

