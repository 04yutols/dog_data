import json
import yaml
import mojimoji  # 半角/全角変換ライブラリ
import re       # 正規表現ライブラリ

# --- ファイル設定 ---
INPUT_FILE = "hotel_data.json"
OUTPUT_FILE = "analysis_results_final.json" # 最終版の出力ファイル名
CONFIG_FILE = "config.yml"

# --- [デバッグ用] 特に注目したい正規化キー (正規化後の文字列を指定) ---
# DEBUG_TARGET_KEY = "エピナール那須" # デバッグ完了のためコメントアウト

# --- [正規化用] 除去する接頭辞/接尾辞のパターン (最終版) ---
PREFIX_SUFFIX_PATTERNS = [
    # 具体的なフレーズ (長くて特徴的なものから先に)
    r'^那須高原ペットと泊まれる宿', r'那須高原ペットと泊まれる宿$',
    r'^ペットと泊まれる宿', r'ペットと泊まれる宿$',
    r'^犬と遊べるペンション', r'犬と遊べるペンション$',
    r'^那須温泉', r'那須温泉$', # [追加] "那須温泉"をピンポイントで除去
    # 一般的な単語
    r'^ホテル', r'ホテル$',
    r'^ぺんしょん', r'ぺんしょん$', r'^ペンション', r'ペンション$',
    r'^旅館', r'旅館$',
    r'^温泉', r'温泉$', # "那須温泉"を除去した後に、単独の"温泉"も除去できるように残す
    r'^高原', r'高原$',
    r'^の宿', r'の宿$',
    r'^ｉｎｎ', r'ｉｎｎ$', r'^イン', r'イン$',
    r'^りぞーと', r'りぞーと$', r'^リゾート', r'リゾート$'
]
PREFIX_SUFFIX_REGEX = re.compile(r'|'.join(PREFIX_SUFFIX_PATTERNS))

def normalize_name(name):
    """
    【v3.2 最終版】ホテル名を正規化（すっぴん化）する関数。
    "那須温泉"などのパターン除去を追加。
    """
    if not name: return ""
    
    normalized = mojimoji.han_to_zen(name, kana=True, ascii=True, digit=True).lower()
    normalized = re.sub(r'[（\(][^（）()]*[）\)]', '', normalized)
    symbols_to_remove = '・＆～★＊！？／♪☆　・＆~★*!?/♪☆ \'ー-'
    normalized = ''.join(c for c in normalized if c not in symbols_to_remove)
    normalized = re.sub(r'\s+', '', normalized) 
    
    for _ in range(3): 
        prev_normalized = normalized
        normalized = PREFIX_SUFFIX_REGEX.sub('', normalized)
        if normalized == prev_normalized: break

    return normalized

def main():
    """
    楽天とじゃらんのデータを統合し、最終版正規化マッチングで名寄せを行い、
    v0.4ロジック + 楽天優先の代表名で「真のあんしんスコア」を算出する最終エンジン。
    """
    print(f"統合分析エンジン v3.2 Final を起動します...") # バージョンアップ

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

    # --- デバッグコードは削除 ---

    # --- 4. グループごとにスコア算出 & 代表名決定 ---
    analysis_results = {}
    print("\n各グループのレビューを統合し、スコア計算を開始します...")

    for norm_key, group_members in hotel_groups.items():
        
        representative_name = None
        rakuten_member = next((m for m in group_members if m['source'] == 'rakuten'), None)
        
        if rakuten_member:
            representative_name = rakuten_member['original_name']
        else:
            representative_name = group_members[0]['original_name']
            
        integrated_reviews = []
        total_reviews = 0
        sources_included = set()
        for member in group_members:
            integrated_reviews.extend(member['reviews'])
            total_reviews += len(member['reviews'])
            sources_included.add(member['source'])

        # --- キーワードカウント処理 ---
        risk_counts = {category: 0 for category in FATAL_RISKS.keys()}
        wow_counts = {category: 0 for category in WOW_FACTORS.keys()}

        if total_reviews > 0:
            all_categories = {**FATAL_RISKS, **WOW_FACTORS}
            for review_text in integrated_reviews:
                found_categories_in_this_review = set()
                for category, keywords in all_categories.items():
                    if category in found_categories_in_this_review: continue
                    for keyword in keywords:
                        if keyword in review_text:
                            if category in risk_counts: risk_counts[category] += 1
                            elif category in wow_counts: wow_counts[category] += 1
                            found_categories_in_this_review.add(category)
                            break
        # ------------------------

        total_risk_points = sum(risk_counts[cat] * abs(SCORE_MAPPING[cat]) for cat in FATAL_RISKS)
        total_wow_points = sum(wow_counts[cat] * SCORE_MAPPING[cat] for cat in WOW_FACTORS)
        risk_rate = (total_risk_points / total_reviews) if total_reviews > 0 else 0
        wow_rate = (total_wow_points / total_reviews) if total_reviews > 0 else 0
        final_score = 50 - (risk_rate * 10) + (wow_rate * 10)
        
        analysis_results[representative_name] = {
            "anshin_score": round(final_score, 1),
            "total_reviews": total_reviews,
            "sources": sorted(list(sources_included)), 
            "risk_details": {"total_risk_points": total_risk_points, "risk_rate": round(risk_rate, 3)},
            "wow_details": {"total_wow_points": total_wow_points, "wow_rate": round(wow_rate, 3)}
        }
        print(f"  - {representative_name} の分析完了。真のあんしんスコア: {final_score:.1f} (Sources: {', '.join(sources_included)})")


    # --- 5. 最終結果を書き出し ---
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(analysis_results, f, ensure_ascii=False, indent=2)
        print("=" * 40)
        print(f"統合分析完了。最終結果を {OUTPUT_FILE} に保存しました。")
        print("=" * 40)
    except IOError as e:
        print(f"エラー: 結果ファイルの書き込みに失敗しました。 {e}")

if __name__ == '__main__':
    main()

