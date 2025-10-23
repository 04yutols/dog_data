import csv
import time
import requests
from bs4 import BeautifulSoup
from difflib import SequenceMatcher # [追加] 文字列類似度を計算するためのライブラリ

# --- 設定項目 ---
INPUT_FILE = 'hotels_raw.csv'
VERIFIED_FILE = 'hotels_verified.csv'
ERROR_LOG_FILE = 'verification_errors.log'
REQUEST_DELAY = 0.5
REQUEST_TIMEOUT = 15
SIMILARITY_THRESHOLD = 0.6 # [追加] これ以上の類似度スコアであれば「一致」とみなす (0.0〜1.0)

def extract_name_from_title(title_text):
    """
    楽天トラベルのtitleタグのテキストから、ホテル名を抽出する。
    """
    try:
        part_after_bracket = title_text.split('】')[1]
        hotel_name = part_after_bracket.split('の詳細')[0]
        return hotel_name.strip()
    except IndexError:
        return None

def main():
    """
    メインの検証処理を実行する。
    """
    print("品質保証(QA)エンジン v2 (類似度分析モデル) を起動します...")
    print(f"入力ファイル: {INPUT_FILE}")
    print(f"類似度しきい値: {SIMILARITY_THRESHOLD}")

    pass_count = 0
    fail_count = 0

    try:
        with open(INPUT_FILE, 'r', encoding='utf-8-sig') as infile, \
             open(VERIFIED_FILE, 'w', newline='', encoding='utf-8') as v_outfile, \
             open(ERROR_LOG_FILE, 'w', encoding='utf-8') as err_outfile:

            reader = csv.reader(infile)
            writer = csv.writer(v_outfile)

            try:
                header = next(reader)
                writer.writerow(header)
            except StopIteration:
                print("エラー: 入力ファイルが空です。")
                return

            for i, row in enumerate(reader, 1):
                if len(row) < 2:
                    error_message = f"[INVALID ROW] 行 {i}: 列数が不足しています。 Row: {row}\n"
                    err_outfile.write(error_message)
                    fail_count += 1
                    continue
                
                csv_name, csv_url = row
                print(f"[{i:03d}] 検証中: {csv_name} ... ", end='', flush=True)

                try:
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 1.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                    }
                    response = requests.get(csv_url, headers=headers, timeout=REQUEST_TIMEOUT)
                    response.raise_for_status()

                    soup = BeautifulSoup(response.content, 'html.parser')
                    if not soup.title or not soup.title.string:
                        raise ValueError("ページのtitleタグが見つかりません。")

                    title_text = soup.title.string
                    web_name = extract_name_from_title(title_text)

                    if web_name is None:
                        raise ValueError(f"想定外のtitle形式です。 Title: '{title_text}'")

                    # --- [変更] Step 3: 検証 (文字列類似度スコア) ---
                    similarity_score = SequenceMatcher(None, csv_name, web_name).ratio()

                    if similarity_score >= SIMILARITY_THRESHOLD:
                        # PASS (検証成功)
                        writer.writerow([csv_name, csv_url])
                        print(f"PASS (Score: {similarity_score:.2f})")
                        pass_count += 1
                    else:
                        # FAIL (不一致)
                        # [変更] ログにスコアも記録する
                        error_message = f"[MISMATCH] Score: {similarity_score:.2f}, CSV Name: {csv_name}, Web Name: {web_name}, URL: {csv_url}\n"
                        err_outfile.write(error_message)
                        print(f"FAIL (Name Mismatch: '{web_name}', Score: {similarity_score:.2f})")
                        fail_count += 1

                except requests.exceptions.RequestException as e:
                    error_message = f"[FETCH FAILED] URL: {csv_url}, Error: {e}\n"
                    err_outfile.write(error_message)
                    print(f"FAIL (Fetch Error)")
                    fail_count += 1
                except ValueError as e:
                    error_message = f"[PARSE FAILED] URL: {csv_url}, Error: {e}\n"
                    err_outfile.write(error_message)
                    print(f"FAIL (Parse Error)")
                    fail_count += 1
                
                time.sleep(REQUEST_DELAY)

    except FileNotFoundError:
        print(f"エラー: 入力ファイル {INPUT_FILE} が見つかりません。")
        return
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")
        return

    print("\n" + "="*40)
    print("品質保証(QA)プロセスが完了しました。")
    print(f"  - 検証成功 (PASS): {pass_count}件 -> {VERIFIED_FILE}")
    print(f"  - 検証失敗 (FAIL): {fail_count}件 -> {ERROR_LOG_FILE}")
    print("="*40)


if __name__ == "__main__":
    main()

