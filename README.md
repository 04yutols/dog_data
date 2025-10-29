# 犬旅リスクスコープ - データパイプライン

## 概要

このプロジェクトは、「犬旅リスクスコープ」サービスのためのデータ収集・分析パイプラインです。
楽天トラベルとじゃらんから犬と泊まれる宿のレビュー情報を収集し、独自のロジックに基づいて「あんしんスコア」を算出します。

## フォルダ構成
```
dog\_travel\_risk\_scope/
├── src/                        \# ソースコード
│   ├── rakuten\_master\_builder.py  \# 楽天マスターリスト生成
│   ├── jalan\_master\_builder.py    \# じゃらんマスターリスト生成
│   ├── review\_scraper.py        \# 統合レビュー収集エンジン
│   └── score\_analyzer.py        \# 統合分析エンジン
│   └── run\_pipeline.py          \# ★全自動実行スクリプト★
│
├── data/                       \# データ
│   ├── input/                  \# 入力ファイル
│   │   ├── search\_urls\_rakuten.txt \# 楽天検索URL
│   │   └── search\_urls\_jalan.txt   \# じゃらん検索URL
│   ├── raw/                    \# 生データ (マスターリスト)
│   │   ├── hotels\_raw\_rakuten.csv
│   │   └── hotels\_raw\_jalan.csv
│   ├── processed/              \# 加工済みデータ (レビューDB)
│   │   └── hotel\_review\_data.json
│   └── output/                 \# 最終成果物
│       └── analysis\_results.json
│
├── config/                     \# 設定ファイル
│   └── config.yml
│
├── logs/                       \# ログファイル (将来用)
│
├── archive/                    \# 古いファイル (任意)
│
├── README.md                   \# このファイル
└── requirements.txt            \# 必要なPythonライブラリ

````

## セットアップ

1.  **リポジトリのクローン:**
    ```bash
    git clone [リポジトリURL]
    cd dog_travel_risk_scope
    ```

2.  **Python環境の準備:**
    Python 3.8 以降を推奨します。仮想環境を作成することをお勧めします。
    ```bash
    python -m venv venv
    ```
    仮想環境をアクティベートします:
    ```bash
    source venv/bin/activate  # Mac/Linux
    venv\Scripts\activate  # Windows
    ```

3.  **必要なライブラリのインストール:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **入力ファイルの準備:**
    * `data/input/search_urls_rakuten.txt` に楽天の検索結果1ページ目のURLを記述します（1行1URL）。
    * `data/input/search_urls_jalan.txt` にじゃらんの検索結果1ページ目のURLを記述します（1行1URL）。

5.  **設定ファイルの確認:**
    * `config/config.yml` のキーワードやスコア設定を確認・調整します。

## 実行方法

**推奨: 全自動実行スクリプトを使用します。**

まず、`src`ディレクトリに移動します。
```bash
cd src
````

次に、パイプラインを実行します。

```bash
python run_pipeline.py
```

これにより、以下のスクリプトが順番に実行されます。

1.  `rakuten_master_builder.py`
2.  `jalan_master_builder.py`
3.  `review_scraper.py`
4.  `score_analyzer.py`

最終的な分析結果は `data/output/analysis_results.json` に出力されます。

-----

**個別実行（デバッグ用）:**

各スクリプトは`src`ディレクトリ内で個別に実行することも可能です。

```bash
cd src
python rakuten_master_builder.py
python jalan_master_builder.py
python review_scraper.py
python score_analyzer.py
```

## 注意点

  * Webスクレイピングは、対象サイトの利用規約に従い、サーバーに過度な負荷をかけないよう注意して実行してください (`REQUEST_DELAY`の調整など)。
  * サイト構造の変更により、CSSセレクタの修正が必要になる場合があります。