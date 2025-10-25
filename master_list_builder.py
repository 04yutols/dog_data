import csv
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

# --- ★書き換える場所★ ---
# 楽天トラベルで「犬と泊まる宿」などを検索した【1ページ目】のURLを貼り付けてください
SEARCH_BASE_URL = "https://search.travel.rakuten.co.jp/ds/undated/search?f_dai=japan&f_chu=tochigi&f_shou=&f_sai=&f_cd=02&f_ptn=tiku&f_latitude=0&f_longitude=0&f_layout=&f_sort=hotel&f_page=1&f_hyoji=30&f_image=1&f_tab=hotel&f_setubi=&f_snow_code=&f_cok=&f_ido=&f_kdo=&f_km=&f_teikei=&f_campaign=&f_disp_type=&f_kin=&f_kin2=&f_landmark_id=&f_squeezes=prm"
# -------------------------

# --- 設定項目 ---
OUTPUT_FILE = 'hotels_raw.csv'       # 生成されるマスターリストのファイル名
REQUEST_DELAY = 1.0                  # 各リクエスト間の待機時間（秒）。サーバー負荷を考慮し、1秒を推奨。
REQUEST_TIMEOUT = 20                 # リクエストのタイムアウト時間（秒）

def main():
    """
    楽天トラベルの検索結果を全ページ巡回し、ホテル名とURLのマスターリストを生成する。
    """
    if not SEARCH_BASE_URL or SEARCH_BASE_URL == "（ここに楽天トラベルの検索結果1ページ目のURLを貼る）":
        print("エラー: 検索URLが設定されていません。SEARCH_BASE_URLを書き換えてください。")
        return

    print("マスターリスト自動構築エンジン v4 (URLパラメータ操作モデル) を起動します...")
    print(f"起点URL: {SEARCH_BASE_URL}")

    all_hotels_data = []
    page_count = 1

    # --- [変更] メインの巡回ループ ---
    # 「次へ」リンクを探すのではなく、ホテルが見つからなくなるまで無限にページ番号を増やし続ける
    while True:
        
        # --- [変更] URLをプログラムで直接生成する ---
        parsed_url = urlparse(SEARCH_BASE_URL)
        query_params = parse_qs(parsed_url.query)
        query_params['f_page'] = [str(page_count)] # ページ番号を上書き
        
        # 新しいクエリ文字列をエンコードし、URLを再構築
        new_query = urlencode(query_params, doseq=True)
        current_url = parsed_url._replace(query=new_query).geturl()

        print(f"\n[ {page_count}ページ目 ] を解析中... URL: {current_url}")

        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            response = requests.get(current_url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')

            hotel_items = soup.select('li.htl-list-card')
            
            # [重要] このページにホテルが1件もなければ、それが最終ページだと判断してループを抜ける
            if not hotel_items:
                print("  -> このページにホテル情報が見つかりませんでした。巡回を終了します。")
                break

            found_on_page = 0
            for item in hotel_items:
                name_element = item.select_one('h2.hotel-list__title-text a')
                
                if name_element and name_element.has_attr('href'):
                    hotel_name = name_element.get_text(strip=True)
                    detail_url = urljoin(current_url, name_element['href'])
                    all_hotels_data.append({'hotel_name': hotel_name, 'url': detail_url})
                    found_on_page += 1
            
            print(f"  -> {found_on_page}件のホテル情報を抽出しました。")
            
            # 次のページへ
            page_count += 1

        except requests.exceptions.RequestException as e:
            print(f"\nエラー: ページの取得に失敗しました。URL: {current_url}, Error: {e}")
            print("処理を中断します。")
            break
        
        time.sleep(REQUEST_DELAY)

    # --- 収集した全データをCSVに書き出し ---
    if not all_hotels_data:
        print("\n1件もホテル情報を収集できませんでした。")
        return

    print("\n" + "="*40)
    print(f"合計 {len(all_hotels_data)}件のホテル情報を収集しました。")
    print(f"CSVファイル ({OUTPUT_FILE}) に書き出します...")

    try:
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['hotel_name', 'url'])
            writer.writeheader()
            writer.writerows(all_hotels_data)
        print("マスターリストの構築が完了しました！")
    except IOError as e:
        print(f"エラー: ファイルの書き込みに失敗しました。 Error: {e}")
    
    print("="*40)

if __name__ == "__main__":
    main()

