CREATE TABLE hotel_analysis_results (
    hotel_name TEXT PRIMARY KEY,                  -- ホテル名 (正規化・代表名決定後)
    anshin_score_alltime NUMERIC(4, 1),           -- 全期間スコア (例: 50.3)
    anshin_score_1year NUMERIC(4, 1),             -- 直近1年スコア
    total_reviews_alltime INTEGER NOT NULL DEFAULT 0, -- 全期間レビュー総数
    total_reviews_1year INTEGER NOT NULL DEFAULT 0,   -- 直近1年レビュー総数
    sources TEXT[],                               -- データソース (例: '{rakuten, jalan}')
    risk_points_alltime INTEGER NOT NULL DEFAULT 0,   -- 全期間リスクポイント合計
    risk_rate_alltime NUMERIC(8, 3) NOT NULL DEFAULT 0.0, -- 全期間リスク率
    wow_points_alltime INTEGER NOT NULL DEFAULT 0,      -- 全期間WOWポイント合計
    wow_rate_alltime NUMERIC(8, 3) NOT NULL DEFAULT 0.0,  -- 全期間WOW率
    risk_points_1year INTEGER NOT NULL DEFAULT 0,       -- 直近1年リスクポイント合計
    risk_rate_1year NUMERIC(8, 3) NOT NULL DEFAULT 0.0,   -- 直近1年リスク率
    wow_points_1year INTEGER NOT NULL DEFAULT 0,        -- 直近1年WOWポイント合計
    wow_rate_1year NUMERIC(8, 3) NOT NULL DEFAULT 0.0,    -- 直近1年WOW率
    last_calculated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP -- この行が最後に更新された日時
);

-- オプション: 検索パフォーマンス向上のためのインデックス
CREATE INDEX idx_hotel_analysis_score_alltime ON hotel_analysis_results (anshin_score_alltime);
CREATE INDEX idx_hotel_analysis_score_1year ON hotel_analysis_results (anshin_score_1year);
CREATE INDEX idx_hotel_analysis_sources ON hotel_analysis_results USING GIN (sources); -- sources 配列での検索用