import subprocess
import sys
import os

# 実行するスクリプトのリスト (この順番で実行される)
SCRIPTS_TO_RUN = [
    "rakuten_master_builder.py",
    "jalan_master_builder.py",
    "review_scraper.py",
    "score_analyzer.py",
]

def run_script(script_name):
    """指定されたPythonスクリプトを実行する"""
    script_path = os.path.join(os.path.dirname(__file__), script_name)
    if not os.path.exists(script_path):
        print(f"エラー: スクリプト '{script_name}' が見つかりません。パス: {script_path}")
        return False
        
    print("\n" + "="*50)
    print(f"実行中: {script_name}")
    print("="*50)
    
    try:
        # subprocessを使って別プロセスとして実行
        # stdoutとstderrを現在のプロセスに表示する
        result = subprocess.run([sys.executable, script_path], check=True, text=True, capture_output=False) # capture_output=Falseで出力をそのまま表示
        print("\n" + "-"*50)
        print(f"完了: {script_name}")
        print("-"*50)
        return True
    except FileNotFoundError:
         print(f"エラー: Pythonインタープリタが見つかりません ({sys.executable})")
         return False
    except subprocess.CalledProcessError as e:
        print("\n" + "!"*50)
        print(f"エラー: {script_name} の実行中にエラーが発生しました。")
        print(f"リターンコード: {e.returncode}")
        # エラー出力は実行時に表示されているはず
        print("!"*50)
        return False
    except Exception as e:
        print("\n" + "!"*50)
        print(f"予期せぬエラー: {script_name} の実行中に問題が発生しました: {e}")
        print("!"*50)
        return False

def main():
    """データパイプライン全体を実行する"""
    start_time = time.time()
    print("データパイプラインを開始します...")
    
    all_success = True
    for script in SCRIPTS_TO_RUN:
        if not run_script(script):
            all_success = False
            print(f"\nパイプラインの実行を '{script}' で中断しました。")
            break # エラーが発生したら中断
            
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print("\n" + "*"*50)
    if all_success:
        print("データパイプラインの全工程が正常に完了しました！")
    else:
        print("データパイプラインの実行中にエラーが発生しました。")
    print(f"総実行時間: {elapsed_time:.2f} 秒")
    print("*"*50)

if __name__ == "__main__":
    # time モジュールをインポート
    import time
    main()

