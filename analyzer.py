import json
import yaml

# --- ファイル設定 ---
INPUT_FILE = "hotel_data.json"
OUTPUT_FILE = "analysis_results_v2.json"
CONFIG_FILE = "config.yml"

def main():
    """
    v0.4ロジックに基づき、正規化された「あんしんスコア」を算出する。
    新しいデータ形式(URLがキー)の hotel_data.json に対応。
    """
    print(f"分析エンジン v0.4.1 (データモデル修正版) を起動します...")

    # --- 1. 設定ファイルの読み込み ---
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        SCORE_MAPPING = config['scores']
        FATAL_RISKS = config['fatal_risks']
        WOW_FACTORS = config['wow_factors']
    except Exception as e:
        print(f"エラー: 設定ファイル({CONFIG_FILE})の読み込みに失敗しました。 {e}")
        return

    # --- 2. レビューデータの読み込み ---
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            all_hotel_data = json.load(f)
    except Exception as e:
        print(f"エラー: データファイル({INPUT_FILE})の読み込みに失敗しました。 {e}")
        return
        
    analysis_results = {}
    print(f"{len(all_hotel_data)}軒のホテルの分析を開始します...")

    # --- [変更] 3. ホテルごとに分析ループ ---
    # 新しいデータ形式 (キー: url, 値: {hotel_name, reviews, ...}) に合わせてループを修正
    for url, data in all_hotel_data.items():
        
        # [変更] 辞書からホテル名とレビューリストを正しく取り出す
        hotel_name = data.get('hotel_name', '不明なホテル')
        reviews = data.get('reviews', [])
        
        total_reviews = len(reviews)
        risk_counts = {category: 0 for category in FATAL_RISKS.keys()}
        wow_counts = {category: 0 for category in WOW_FACTORS.keys()}

        if total_reviews > 0:
            for review_text in reviews:
                found_categories_in_this_review = set()
                all_categories = {**FATAL_RISKS, **WOW_FACTORS}
                for category, keywords in all_categories.items():
                    if category in found_categories_in_this_review: continue
                    for keyword in keywords:
                        if keyword in review_text:
                            if category in risk_counts: risk_counts[category] += 1
                            elif category in wow_counts: wow_counts[category] += 1
                            found_categories_in_this_review.add(category)
                            break
        
        # --- v0.4スコアリングロジック (ここは変更なし) ---
        total_risk_points = sum(risk_counts[cat] * abs(SCORE_MAPPING[cat]) for cat in FATAL_RISKS)
        total_wow_points = sum(wow_counts[cat] * SCORE_MAPPING[cat] for cat in WOW_FACTORS)
        risk_rate = (total_risk_points / total_reviews) if total_reviews > 0 else 0
        wow_rate = (total_wow_points / total_reviews) if total_reviews > 0 else 0
        final_score = 50 - (risk_rate * 10) + (wow_rate * 10)
        
        # --- [変更] 結果の格納 (キーをホテル名にする) ---
        analysis_results[hotel_name] = {
            "anshin_score": round(final_score, 1),
            "total_reviews": total_reviews,
            "risk_details": {"total_risk_points": total_risk_points, "risk_rate": round(risk_rate, 3)},
            "wow_details": {"total_wow_points": total_wow_points, "wow_rate": round(wow_rate, 3)}
        }
        print(f"  - {hotel_name} の分析完了。新あんしんスコア: {final_score:.1f}")

    # --- 4. 最終結果を書き出し ---
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(analysis_results, f, ensure_ascii=False, indent=2)
        print("=" * 40)
        print(f"分析完了。結果を {OUTPUT_FILE} に保存しました。")
        print("=" * 40)
    except IOError as e:
        print(f"エラー: 結果ファイルの書き込みに失敗しました。 {e}")

if __name__ == '__main__':
    main()
