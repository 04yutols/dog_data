import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urlparse, urlunparse

# --- ★書き換える場所★ ---
# 検証したい「じゃらん」の宿の【クチコミ・評価ページ】のURLをどちらか一方貼り付けてください
# (1年以内 or それ以前、どちらでも自動で両方を取得します)
TARGET_URL = "https://www.jalan.net/yad373723/kuchikomi/?screenId=UWW3001&yadNo=373723&roomCount=1&adultNum=2&stayCount=1&rootCd=55&smlCd=080902&distCd=01&ccnt=lean-kuchikomi-tab"
# -------------------------

def scrape_review_section(start_url, headers):
    """
    【現場部隊】指定されたセクション（例：1年以内）の全ページを巡回し、レビューを返す。
    """
    section_reviews = []
    page_num = 1
    last_page_first_review_text = None

    while True:
        if page_num == 1:
            current_url = start_url
            print(f"\n[ 1ページ目 ] を解析中...")
        else:
            # URLのパス部分を解析して、ページ番号を挿入
            parsed_url = urlparse(start_url)
            path = parsed_url.path
            
            # パスを 'kuchikomi' で分割し、ページ番号を挿入
            # 例: /yad373723/kuchikomi/ -> /yad373723/kuchikomi/2.HTML
            # 例: /yad373723/kuchikomi/archive/ -> /yad373723/kuchikomi/archive/2.HTML
            if path.endswith('/'):
                path = path.rstrip('/')
            
            # 既に .HTML が含まれている場合はそれを基準に置換
            if f"{page_num-1}.HTML" in path:
                 new_path = path.replace(f"{page_num-1}.HTML", f"{page_num}.HTML")
            else: # .HTML がない場合 (1->2ページ目)
                 new_path = f"{path}/{page_num}.HTML"

            # 新しいパスでURLを再構築
            current_url = urlunparse(parsed_url._replace(path=new_path))
            print(f"\n[ {page_num}ページ目 ] を解析中...")

        print(f"URL: {current_url}")

        try:
            response = requests.get(current_url, headers=headers, timeout=15)
            # 404 Not Found はエラーとせず、ページの終端とみなす
            if response.status_code == 404:
                print("-> 404 Not Found。このセクションの最終ページと判断します。")
                break
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"-> ページの取得に失敗しました。{e}")
            break

        soup = BeautifulSoup(response.content, 'html.parser')
        review_elements = soup.select('p.jlnpc-kuchikomiCassette__postBody')

        if not review_elements:
            print("-> このページにレビューが見つかりませんでした。このセクションの取得を完了します。")
            break

        current_page_first_review_text = review_elements[0].get_text(strip=True)
        if current_page_first_review_text == last_page_first_review_text:
            print("-> 前のページと同じ内容を検出しました。このセクションの取得を完了します。")
            break
        
        last_page_first_review_text = current_page_first_review_text

        print(f"-> {len(review_elements)}件のレビューを発見しました。")
        for review in review_elements:
            section_reviews.append(review.get_text(strip=True))
        
        page_num += 1
        print("-> 1秒待機します...")
        time.sleep(1)

    return section_reviews


def main():
    """
    【司令塔】全体の処理を統括する。
    """
    if not TARGET_URL or TARGET_URL == "（ここにじゃらんのレビューページのURLを貼る）":
        print("エラー: ターゲットURLが設定されていません。URLを書き換えてから実行してください。")
        return

    print(f"じゃらんのレビューページへのアクセスを開始します: {TARGET_URL}")

    headers = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5.0 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36" }
    all_reviews = []
    
    # --- [変更] 2つのURLを自動生成 ---
    parsed_target_url = urlparse(TARGET_URL)
    path = parsed_target_url.path
    
    if "/archive/" in path:
        # アーカイブURLが指定された場合
        archive_url = TARGET_URL
        recent_path = path.replace("/archive/", "/")
        recent_url = urlunparse(parsed_target_url._replace(path=recent_path))
    else:
        # 1年以内のURLが指定された場合
        recent_url = TARGET_URL
        # pathの末尾のスラッシュを保証
        if not path.endswith('/'):
            path += '/'
        archive_path = path + 'archive/'
        archive_url = urlunparse(parsed_target_url._replace(path=archive_path))

    # --- 1. 「1年以内のレビュー」を取得 ---
    print("\n" + "=" * 50)
    print("【フェーズ1】1年以内のレビューを取得します...")
    print(f"起点URL: {recent_url}")
    print("=" * 50)
    recent_reviews = scrape_review_section(recent_url, headers)
    all_reviews.extend(recent_reviews)

    # --- 2. 「1年以上前のレビュー」を取得 ---
    print("\n" + "=" * 50)
    print("【フェーズ2】1年以上前のレビュー（アーカイブ）を取得します...")
    print(f"起点URL: {archive_url}")
    print("=" * 50)
    archive_reviews = scrape_review_section(archive_url, headers)
    all_reviews.extend(archive_reviews)
    
    # --- 最終結果の表示 ---
    print("\n" + "=" * 50)
    print(f"【最終結果】合計 {len(all_reviews)}件のレビューを両方のセクションから取得しました！")
    print("=" * 50)

    for i, review_text in enumerate(all_reviews, 1):
        print(f"\n【レビュー {i}】")
        print(review_text)
        print("-" * 30)

if __name__ == "__main__":
    main()

