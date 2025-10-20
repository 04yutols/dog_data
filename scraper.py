import requests
from bs4 import BeautifulSoup
import time
import json # JSONファイルを扱うために import

# --- ★書き換える場所★ ---
# 1ページ目 (f_next=0 が含まれるURL) を貼ってください
TARGET_URL = "https://review.travel.rakuten.co.jp/hotel/voice/109107/?f_time=&f_keyword=&f_age=0&f_sex=0&f_mem1=0&f_mem2=0&f_mem3=0&f_mem4=0&f_mem5=0&f_teikei=&f_version=2&f_static=1&f_point=0&f_sort=0&f_jrdp=0&f_next=0" 
OUTPUT_FILE = "reviews.json" # 保存するファイル名
# -------------------------

def main():
    if not TARGET_URL or TARGET_URL == "（ここに楽天トラベルのレビューページのURLを貼る）":
        print("エラー: ターゲットURLが設定されていません。URLを書き換えてから実行してください。")
        return
    
    if "f_next=0" not in TARGET_URL:
        print("警告: ターゲットURLに 'f_next=0' が含まれていません。")

    print(f"ターゲットURLの解析を開始します: {TARGET_URL}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    }

    all_reviews = [] # [追加] 全レビュー本文を格納するリスト
    current_offset = 0
    page_counter = 0 

    while True:
        page_counter += 1 
        
        if current_offset == 0:
            url_to_fetch = TARGET_URL
            print(f"\n[ {page_counter}ページ目 ] (offset=0) を解析中...")
        else:
            url_to_fetch = TARGET_URL.replace("f_next=0", f"f_next={current_offset}")
            print(f"\n[ {page_counter}ページ目 ] (offset={current_offset}) を解析中...")
        
        print(f"URL: {url_to_fetch}")

        try:
            response = requests.get(url_to_fetch, headers=headers, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"エラー: HTMLの取得に失敗しました。 {e}")
            break 

        soup = BeautifulSoup(response.content, 'html.parser')
        review_elements = soup.select(".commentSentence")
        
        if not review_elements:
            print("このページにレビューが見つかりませんでした。解析を終了します。")
            page_counter -= 1 
            break
        
        page_review_count = len(review_elements)
        print(f"{page_review_count}件のレビューを解析します。")

        # [変更] キーワードカウントの代わりに、リストに本文を追加
        for review in review_elements:
            review_text = review.get_text(strip=True)
            all_reviews.append(review_text)
        
        current_offset += 20
        
        print("1秒待機します...")
        time.sleep(1) # サーバー負荷のため、ここは 1秒 のまま保持

    # --- ループ終了後、ファイルに保存 ---
    print("=" * 30)
    print("【取得結果】")
    print(f"処理した総ページ数: {page_counter} ページ")
    print(f"取得した総レビュー数: {len(all_reviews)} 件")
    
    try:
        # [追加] 取得した全レビューをJSONファイルに書き出す
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_reviews, f, ensure_ascii=False, indent=2)
        print(f"全レビューを {OUTPUT_FILE} に保存しました。")
    except IOError as e:
        print(f"エラー: ファイルの書き込みに失敗しました。 {e}")
    
    print("=" * 30)

if __name__ == "__main__":
    main()