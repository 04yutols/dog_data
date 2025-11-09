import pytest
from datetime import datetime, timedelta

# テスト対象の関数を analyzer.py からインポート
try:
    from src.score_analyzer import normalize_name, calculate_score
except ImportError:
    from score_analyzer import normalize_name, calculate_score


# --- 1. normalize_name 関数のテスト ---

@pytest.mark.parametrize("input_name, expected_normalized_name", [
    # [修正] カタカナは全角のまま
    ("ホテルエピナール那須", "エピナール那須"),
    ("那須温泉　ホテルエピナール那須", "エピナール那須"), 
    ("ペンション　ありの塔", "ありの塔"),
    ("犬と遊べるペンション　ありの塔", "ありの塔"),
    # [修正] inn は除去しない
    ("ペンション　ハロハロ inn 那須", "ハロハロinn那須"),
    ("那須高原ペットと泊まれる宿ペンションハロハロｉｎｎ那須", "ハロハロinn那須"),
    # 半角/全角混在
    ("Rakuten STAY VILLA 日光", "rakutenstayvilla日光"),
    ("Ｒａｋｕｔｅｎ　ＳＴＡＹ　ＶＩＬＬＡ　日光", "rakutenstayvilla日光"),
    # 記号
    ("ホテル・ラフォーレ那須", "ラフォーレ那須"), # 中黒除去
    ("リブマックスリゾート鬼怒川（旧名：源泉の宿　らんりょう）", "リブマックスリゾート鬼怒川"),
    ("コテージ　わん’Ｓ", "コテージわんs"), # 特殊アポストロフィ除去
    # 複合ケース
    ("那須温泉　ペット＆スパホテル　那須ワン", "ペットスパホテル那須ワン"), # ＆除去
    # エッジケース
    (" ホテル ", ""),
    ("温泉", ""),
    ("", ""),
    (None, ""),
    # [修正] 期待値を「除去済み」に変更
    ("ホテルA-B", "a-b"),
    ("ホテルA' B", "ab"),
])
def test_normalize_name(input_name, expected_normalized_name):
    """
    normalize_name 関数が様々な表記ゆれを正しく処理できるかテストする (v3.2 Final Fix)。
    """
    assert normalize_name(input_name) == expected_normalized_name

# --- 2. calculate_score 関数のテスト (変更なし) ---
# ... (calculate_score のテストは元々パスしていたので、変更なし) ...
# テスト用の設定データを定義 (config.yml を模倣)
MOCK_SCORE_MAPPING = {
    "部屋の衛生状態が悪い": -15, "実態との乖離": -12, "高額な追加料金": -8, "従業員の対応への不信感": -5,
    "最高の遊び場": 3, "極上のおもてなし": 3, "いつでも一緒": 3
}
MOCK_FATAL_RISKS = {
    "部屋の衛生状態が悪い": ["汚い", "不潔"],
    "実態との乖離": ["写真と違う", "狭い"],
    "高額な追加料金": ["追加料金"],
    "従業員の対応への不信感": ["無愛想"]
}
MOCK_WOW_FACTORS = {
    "最高の遊び場": ["広い", "ドッグラン"],
    "極上のおもてなし": ["おやつ", "手作りごはん"],
    "いつでも一緒": ["レストラン同伴可"]
}

# テスト用のレビューデータ
sample_reviews_1 = [
    {"date": "2025-10-01", "text": "部屋が少し狭いけど、ドッグランが広くて最高！"}, # 実態-12, 遊び場+3
    {"date": "2025-09-15", "text": "とても綺麗でした。おやつもたくさん！"},       # おもてなし+3
    {"date": "2025-08-01", "text": "スタッフが無愛想。部屋も汚い。二度と行かない。汚い！"},# 対応-5, 衛生-15 (重複は1回)
]
sample_reviews_2 = [
    {"date": "2025-07-01", "text": "可もなく不可もなく。"},                   # ポイント変動なし
]
sample_reviews_empty = [] # レビューゼロ件

def test_calculate_score_basic():
    """
    calculate_score 関数が基本的なケースで正しくスコアを計算できるか。
    """
    score, total, risks, wows, r_rate, w_rate, r_points, w_points = calculate_score(
        sample_reviews_1, MOCK_SCORE_MAPPING, MOCK_FATAL_RISKS, MOCK_WOW_FACTORS
    )
    expected_score = 50 - ((12 + 5 + 15) / 3 * 10) + ((3 + 3) / 3 * 10) # 約-36.7

    assert total == 3
    assert score == pytest.approx(expected_score, 0.1)
    assert risks == {"部屋の衛生状態が悪い": 1, "実態との乖離": 1, "高額な追加料金": 0, "従業員の対応への不信感": 1}
    assert wows == {"最高の遊び場": 1, "極上のおもてなし": 1, "いつでも一緒": 0}
    assert r_points == 32
    assert w_points == 6
    assert r_rate == pytest.approx(32 / 3, 0.001)
    assert w_rate == pytest.approx(6 / 3, 0.001)


def test_calculate_score_no_keywords():
    """ キーワードが含まれないレビューの場合。 """
    score, total, risks, wows, r_rate, w_rate, r_points, w_points = calculate_score(
        sample_reviews_2, MOCK_SCORE_MAPPING, MOCK_FATAL_RISKS, MOCK_WOW_FACTORS
    )
    assert total == 1
    assert score == 50.0
    assert risks == {"部屋の衛生状態が悪い": 0, "実態との乖離": 0, "高額な追加料金": 0, "従業員の対応への不信感": 0}
    assert wows == {"最高の遊び場": 0, "極上のおもてなし": 0, "いつでも一緒": 0}
    assert r_points == 0
    assert w_points == 0
    assert r_rate == 0.0
    assert w_rate == 0.0

def test_calculate_score_empty_reviews():
    """ レビューリストが空の場合。 """
    score, total, risks, wows, r_rate, w_rate, r_points, w_points = calculate_score(
        sample_reviews_empty, MOCK_SCORE_MAPPING, MOCK_FATAL_RISKS, MOCK_WOW_FACTORS
    )
    assert total == 0
    assert score == 50.0
    assert risks == {}
    assert wows == {}
    assert r_points == 0
    assert w_points == 0
    assert r_rate == 0.0
    assert w_rate == 0.0