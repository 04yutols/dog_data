import json
import yaml
import mojimoji  # 半角/全角変換ライブラリ
import re       # 正規表現ライブラリ
from datetime import datetime, timedelta

# --- ファイル設定 ---
INPUT_FILE = "../data/processed/hotel_review_data.json"
OUTPUT_FILE = "../data/output/analysis_results.json"
CONFIG_FILE = "../config/config.yml"

# --- [正規化用] 除去する接頭辞/接尾辞のパターン (最終版) ---
PREFIX_SUFFIX_PATTERNS = [
    # 具体的なフレーズ
    r'^那須高原ペットと泊まれる宿', r'那須高原ペットと泊まれる宿$',
    r'^ペットと泊まれる宿', r'ペットと泊まれる宿$',
    r'^犬と遊べるペンション', r'犬と遊べるペンション$',
    r'^那須温泉', r'那須温泉$',
    # 一般的な単語
    r'^ホテル', r'ホテル$',
    r'^ぺんしょん', r'ぺんしょん$', r'^ペンション', r'ペンション$',
    r'^旅館', r'旅館$',
    r'^温泉', r'温泉$',
    r'^高原', r'高原$',
    r'^の宿', r'の宿$',
    r'^ｉｎｎ', r'ｉｎｎ$', r'^イン', r'イン$',
    r'^りぞーと', r'りぞーと$', r'^リゾート', r'リゾート$'
]
PREFIX_SUFFIX_REGEX = re.compile(r'|'.join(PREFIX_SUFFIX_PATTERNS))

def normalize_name(name):
    """
    【v3.2 Final Fix】ホテル名を正規化（すっぴん化）する関数。
    バグ修正: 記号除去を強化。
    """
    if not name: return ""
    
    normalized = mojimoji.han_to_zen(name, kana=True, ascii=True, digit=True).lower()
    normalized = re.sub(r'[（\(][^（）()]*[）\)]', '', normalized)
    
    # --- [修正] 記号除去リストにハイフンと両方のアポストロフィを追加 ---
    symbols_to_remove = '・＆～★＊！？／♪☆　・＆~★*!?/♪☆-' # 長音符 'ー' は除去しない
    symbols_to_remove += ' '  # 半角スペース
    symbols_to_remove += '’' # 特殊アポストロフィ
    symbols_to_remove += '\'' # 通常アポストロフィ
    symbols_to_remove += '-'  # ハイフン
    normalized = ''.join(c for c in normalized if c not in symbols_to_remove)
    # -----------------------------------------------------------------
    
    # [修正] 空白除去を先に実行
    normalized = re.sub(r'\s+', '', normalized) 
    
    for _ in range(3): 
        prev_normalized = normalized
        normalized = PREFIX_SUFFIX_REGEX.sub('', normalized)
        if normalized == prev_normalized: break
    
    # [修正] カタカナは全角のまま、英数のみ半角に戻す
    normalized = mojimoji.zen_to_han(normalized, ascii=True, digit=True, kana=False) 

    return normalized

def calculate_score(reviews_list, score_mapping, fatal_risks, wow_factors):
    """
    与えられたレビューリストからv0.4スコアを計算する関数。
    """
    total_reviews = len(reviews_list)
    if total_reviews == 0:
        return 50.0, 0, {}, {}, 0.0, 0.0, 0, 0 # score, total_reviews, risk_counts, wow_counts, risk_rate, wow_rate, risk_points, wow_points

    risk_counts = {category: 0 for category in fatal_risks.keys()}
    wow_counts = {category: 0 for category in wow_factors.keys()}
    all_categories = {**fatal_risks, **wow_factors}

    for review_entry in reviews_list:
        review_text = review_entry.get("text", "")
        if not review_text: continue

        found_categories_in_this_review = set()
        for category, keywords in all_categories.items():
            if category in found_categories_in_this_review: continue
            for keyword in keywords:
                if keyword in review_text:
                    score_value = score_mapping.get(category)
                    if score_value is None: continue 

                    if category in risk_counts: risk_counts[category] += 1
                    elif category in wow_counts: wow_counts[category] += 1
                    found_categories_in_this_review.add(category)
                    break

    total_risk_points = sum(risk_counts[cat] * abs(score_mapping.get(cat, 0)) for cat in fatal_risks if score_mapping.get(cat) is not None)
    total_wow_points = sum(wow_counts[cat] * score_mapping.get(cat, 0) for cat in wow_factors if score_mapping.get(cat) is not None)
    
    risk_rate = (total_risk_points / total_reviews) if total_reviews > 0 else 0
    wow_rate = (total_wow_points / total_reviews) if total_reviews > 0 else 0
    final_score = 50 - (risk_rate * 10) + (wow_rate * 10)

    return round(final_score, 1), total_reviews, risk_counts, wow_counts, round(risk_rate, 3), round(wow_rate, 3), total_risk_points, total_wow_points


def main():
    """
    日付付きレビューデータを読み込み、全期間スコアと直近1年スコアを算出する。
    """
    print(f"時間軸分析エンジン v4.0.1 (バグ修正版) を起動します...")

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

    if not isinstance(all_hotel_data, dict):
        print(f"エラー: {INPUT_FILE} のデータ形式が不正です。")
        return

    # --- 3. ホテルマッチング（名寄せ） ---
    print("ホテル名の正規化とグループ化を開始します...")
    hotel_groups = {}
    for unique_id, data in all_hotel_data.items():
        original_name = data.get('hotel_name')
        if not original_name: continue
        normalized_key = normalize_name(original_name)
        if not normalized_key: continue

        if normalized_key not in hotel_groups:
            hotel_groups[normalized_key] = []
        hotel_groups[normalized_key].append({
            'unique_id': unique_id,
            'original_name': original_name,
            'source': data.get('source', 'unknown'),
            'reviews': data.get('reviews', [])
        })
    print(f"-> {len(all_hotel_data)}件のデータを{len(hotel_groups)}グループにまとめました。")

    # --- 4. グループごとにスコア算出 (全期間 + 1年) ---
    analysis_results = {}
    print("\n各グループのレビューを統合し、スコア計算を開始します...")

    one_year_ago = datetime.now() - timedelta(days=365)

    for norm_key, group_members in hotel_groups.items():

        representative_name = None
        rakuten_member = next((m for m in group_members if m['source'] == 'rakuten'), None)
        representative_name = rakuten_member['original_name'] if rakuten_member else group_members[0]['original_name']

        integrated_reviews_with_dates = []
        sources_included = set()
        for member in group_members:
            valid_reviews = [r for r in member['reviews'] if isinstance(r, dict) and 'date' in r and 'text' in r]
            integrated_reviews_with_dates.extend(valid_reviews)
            sources_included.add(member['source'])

        # --- 全期間スコア算出 ---
        score_all, total_all, risks_all_counts, wows_all_counts, risk_rate_all, wow_rate_all, total_risk_points_all, total_wow_points_all = calculate_score(
            integrated_reviews_with_dates, SCORE_MAPPING, FATAL_RISKS, WOW_FACTORS
        )

        # --- 1年以内レビュー抽出 & スコア算出 ---
        one_year_reviews = []
        for review in integrated_reviews_with_dates:
            if review.get('date'):
                 try:
                      review_date = datetime.strptime(review['date'], '%Y-%m-%d')
                      if review_date >= one_year_ago:
                           one_year_reviews.append(review)
                 except (ValueError, TypeError):
                      continue

        score_1yr, total_1yr, risks_1yr_counts, wows_1yr_counts, risk_rate_1yr, wow_rate_1yr, total_risk_points_1yr, total_wow_points_1yr = calculate_score(
            one_year_reviews, SCORE_MAPPING, FATAL_RISKS, WOW_FACTORS
        )

        analysis_results[representative_name] = {
            "anshin_score_alltime": score_all,
            "anshin_score_1year": score_1yr,
            "total_reviews_alltime": total_all,
            "total_reviews_1year": total_1yr,
            "sources": sorted(list(sources_included)),
            "risk_details_alltime": {"total_risk_points": total_risk_points_all, "risk_rate": risk_rate_all},
            "wow_details_alltime": {"total_wow_points": total_wow_points_all, "wow_rate": wow_rate_all},
            "risk_details_1year": {"total_risk_points": total_risk_points_1yr, "risk_rate": risk_rate_1yr},
            "wow_details_1year": {"total_wow_points": total_wow_points_1yr, "wow_rate": wow_rate_1yr}
        }
        print(f"  - {representative_name} の分析完了。スコア(全期間): {score_all:.1f}, スコア(1年): {score_1yr:.1f} (Sources: {', '.join(sources_included)})")

    # --- 5. 最終結果を書き出し ---
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(analysis_results, f, ensure_ascii=False, indent=2)
        print("=" * 40)
        print(f"時間軸分析完了。最終結果を {OUTPUT_FILE} に保存しました。")
        print("=" * 40)
    except IOError as e:
        print(f"エラー: 結果ファイルの書き込みに失敗しました。 {e}")

if __name__ == '__main__':
    main()

