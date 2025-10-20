import json # JSONファイルを扱うために import

# --- 解析したいリスクキーワード ---
RISK_KEYWORDS = ["汚い", "狭い"]
# -------------------------
INPUT_FILE = "reviews.json" # 読み込むファイル名
# -------------------------

def main():
    print(f"{INPUT_FILE} を読み込んで分析を開始します...")

    # --- [追加] JSONファイルの読み込み ---
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            all_reviews = json.load(f)
        print(f"ファイルの読み込みに成功しました。総レビュー数: {len(all_reviews)}件")
    except FileNotFoundError:
        print(f"エラー: {INPUT_FILE} が見つかりません。")
        print("先に scraper.py を実行して、レビューを取得してください。")
        return
    except json.JSONDecodeError:
        print(f"エラー: {INPUT_FILE} の形式が正しくありません。")
        return
    except Exception as e:
        print(f"エラー: ファイル読み込み中に問題が発生しました。 {e}")
        return
    # -----------------------------------

    if not all_reviews:
        print("ファイル内にレビューデータがありません。")
        return

    # --- 分析処理（ここは元のコードとほぼ同じ） ---
    keyword_counts = {keyword: 0 for keyword in RISK_KEYWORDS}

    for review_text in all_reviews:
        for keyword in RISK_KEYWORDS:
            if keyword in review_text:
                keyword_counts[keyword] += 1
    
    print("-" * 30)
    print("【解析結果】")
    print(f"解析した総レビュー数: {len(all_reviews)}件")
    for keyword, count in keyword_counts.items():
        print(f"・「{keyword}」が含まれていたレビュー数: {count}件")
    print("-" * 30)
    
    print("\nキーワードを変更したい場合は、このスクリプト (analyzer.py) の")
    print("RISK_KEYWORDS リストを書き換えて、もう一度実行してください。")


if __name__ == "__main__":
    main()