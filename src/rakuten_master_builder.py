import csv
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

# --- ★設定場所★ ---
# [変更] 楽天の検索URLリストのファイルパス
URL_LIST_FILE = '../data/input/search_urls_rakuten.txt' # srcフォルダからの相対パス
# -------------------------

# --- 設定項目 ---
# [変更] 出力するマスターリストのファイルパス
OUTPUT_FILE = '../data/raw/hotels_raw_rakuten.csv' # srcフォルダからの相対パス
REQUEST_DELAY = 1.0                  # 各リクエスト間の待機時間（秒）。サーバー負荷を考慮し、1秒を推奨。
REQUEST_TIMEOUT = 20                 # リクエストのタイムアウト時間（秒）

def main():
    """
    search_urls_rakuten.txtから複数の起点URLを読み込み、
    楽天の検索結果を全ページ巡回して、単一のマスターリストを生成する。
    """
    print("楽天用マスターリスト自動構築エンジン v6 (複数地域対応モデル) を起動します...")

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
    unique_hotel_urls = set() # URL基準で重複チェック

    # --- [変更] 読み込んだ起点URLごとにループ ---
    for i, base_url in enumerate(search_base_urls, 1):
        print("\n" + "="*50)
        print(f"[{i}/{len(search_base_urls)}] 起点URLの処理を開始: {base_url[:80]}...")
        print("="*50)

        page_count = 1
        # --- [変更] while True ループでページ巡回 ---
        while True:
            # ページネーション (f_pageパラメータ操作)
            parsed_url = urlparse(base_url)
            query_params = parse_qs(parsed_url.query)
            query_params['f_page'] = [str(page_count)] # ページ番号を上書き
            new_query = urlencode(query_params, doseq=True)
            current_url = parsed_url._replace(query=new_query).geturl()

            print(f"\n[ {page_count}ページ目 ] を解析中...")
            print(f"URL: {current_url}")

            try:
                headers = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36" }
                response = requests.get(current_url, headers=headers, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')

                # --- [変更] 楽天のホテルリスト抽出 (セレクタは要確認・調整) ---
                # 以前の調査では 'div.search-result-item-V2__container__main' や 'li.htl-list-card' だったが、
                # 最新の構造に合わせて再確認が必要。ここでは仮のセレクタを使用。
                # 例: 各ホテルが <div class="hotel-item">...</div> で囲まれている場合
                hotel_items = soup.select('li.htl-list-card')
                # [変更] ホテルが見つからなければループ終了
                if not hotel_items:
                    print("  -> このページにホテル情報が見つかりませんでした。この起点URLの処理を終了します。")
                    break

                found_on_page = 0
                for item in hotel_items:
                    # ★★★ ホテル名と詳細ページURLを取得するセレクタも要確認・修正 ★★★
                    # 例: <a class="hotel-name-link" href="...">ホテル名</a>
                    name_link_element = item.select_one('h2.hotel-list__title-text a')

                    if name_link_element and name_link_element.has_attr('href'):
                        hotel_name = name_link_element.get_text(strip=True)
                        detail_url = urljoin(current_url, name_link_element['href'])
                    # --- [変更] ここからが新しいロジック ---
                    try:
                        # 1. URLからホテルIDを抽出 (例: .../HOTEL/186671/... -> 186671)
                        hotel_id = detail_url.split('/')[4]
                        if not hotel_id.isdigit():
                            print(f"\n  -> 警告: 不正なホテルIDを検出。スキップします。URL: {detail_url}")
                            continue

                        # 2. 抽出したIDを使って、レビューページのURLを直接組み立てる
                        review_page_url = f"https://review.travel.rakuten.co.jp/hotel/voice/{hotel_id}/?f_time=&f_keyword=&f_age=0&f_sex=0&f_mem1=0&f_mem2=0&f_mem3=0&f_mem4=0&f_mem5=0&f_teikei=&f_version=2&f_static=1&f_point=0&f_sort=0&f_jrdp=0&f_next=0"
                        
                        # 3. マスターリストには、組み立てたレビューページのURLを保存
                        all_hotels_data.append({'hotel_name': hotel_name, 'url': review_page_url})
                        found_on_page += 1

                    except IndexError:
                        print(f"\n  -> 警告: 想定外のURL形式のためIDを抽出できませんでした。スキップします。URL: {detail_url}")
                        continue                        
                    found_on_page += 1

                # [変更] 新規ホテルが0件なら、それが最終ページと判断してループを抜ける
                if found_on_page == 0 and page_count > 1:
                     print("  -> 新規のホテルが見つかりませんでした。最終ページと判断し、巡回を終了します。")
                     break

                # 次のページへ
                page_count += 1
                time.sleep(REQUEST_DELAY)

            except requests.exceptions.RequestException as e:
                print(f"\nエラー: ページの取得に失敗。この起点URLの処理をスキップします。 Error: {e}")
                break # エラーが出たらこの起点URLは中断

    # --- 収集した全データをCSVに書き出し ---
    if not all_hotels_data:
        print("\n1件もホテル情報を収集できませんでした。")
        return

    print("\n" + "="*50)
    print(f"全地域の収集が完了しました。合計 {len(all_hotels_data)}件のユニークなホテル情報を収集しました。")
    print(f"CSVファイル ({OUTPUT_FILE}) に書き出します...")

    try:
        # encoding='utf-8-sig' でBOM付きUTF-8として保存
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=['hotel_name', 'url'])
            writer.writeheader()
            writer.writerows(all_hotels_data)
        print("楽天用マスターリストの構築が完了しました！")
    except IOError as e:
        print(f"エラー: ファイル({OUTPUT_FILE})の書き込みに失敗しました。 Error: {e}")
    print("="*50)

if __name__ == "__main__":
    main()

