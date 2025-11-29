#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
GraphML (GDELT KG) から
  日付 × 国コード × リスク種別
のイベント件数を集計し、baseline からのスパイクを検知するスクリプト。

使い方:
  python graphml_to_spikes.py gdelt_full_kg.graphml \
      --as-of 2025-11-24 \
      --baseline-days 7 \
      --min-baseline-mean 1.0 \
      --min-delta-rel 1.0

出力:
  spike_candidates.csv にスパイク一覧を書き出しつつ、
  標準出力にも先頭数行を表示。
"""

import argparse
from datetime import datetime, timedelta

import networkx as nx
import pandas as pd

# ==== ノード種別 & 属性名 ====
NODE_ATTR_TYPE = "type"
NODE_ATTR_LABEL = "label"
NODE_ATTR_DATE = "date"
NODE_ATTR_COUNTRY = "country"
NODE_ATTR_CAMEO = "cameo"

EVENT_NODE_TYPE = "Event"
LOCATION_NODE_TYPE = "Location"
THEME_NODE_TYPE = "Theme"

# ==== リスク種別 ====
RISK_MILITARY = "MILITARY"
RISK_CIVIL_UNREST = "CIVIL_UNREST"
RISK_POLITICAL = "POLITICAL"
RISK_ECONOMIC = "ECONOMIC"
RISK_NATURAL_DISASTER = "NATURAL_DISASTER"
RISK_OTHER = "OTHER"  # 分類できなかったもの

# ==== ログ用ヘルパ ====
def log(msg: str) -> None:
    print(f"[graphml_to_spikes] {msg}")


# ==== CAMEO / Theme からリスク種別をざっくり分類する ====


def _classify_by_cameo(cameo: str) -> str | None:
    """
    CAMEO コード (例: '061', '141') からざっくり分類。
    ここはかなりラフなヒューリスティックなので、必要に応じて調整してOK。
    CAMEO の root code (先頭2桁) を使う。
    """
    if not cameo:
        return None

    cameo = cameo.strip()
    root_str = cameo[:2]
    if not root_str.isdigit():
        return None

    root = int(root_str)

    # 参考: CAMEO root
    #  14 PROTEST
    #  15 EXHIBIT FORCE POSTURE
    #  16 REDUCE RELATIONS
    #  17 COERCE
    #  18 ASSAULT
    #  19 FIGHT
    #  20 MASS VIOLENCE
    if root in (18, 19, 20, 15, 17):
        return RISK_MILITARY
    if root == 14:
        return RISK_CIVIL_UNREST
    if root in (10, 11, 12, 13, 16):
        return RISK_POLITICAL
    if root in (6, 7):  # material cooperation / aid など
        return RISK_ECONOMIC

    # その他はここでは未分類
    return None


def _classify_by_theme_labels(theme_labels: list[str]) -> str | None:
    """
    Theme ノードのラベルに含まれるキーワードから分類。
    GDELT の Theme は環境ごとに結構ブレるので、
    必要に応じてキーワードを増やしていく想定。
    """
    if not theme_labels:
        return None

    # 大文字にしてから判定
    up = [t.upper() for t in theme_labels]

    def contains_any(substrings: list[str]) -> bool:
        return any(any(s in t for s in substrings) for t in up)

    # 自然災害
    if contains_any(
        [
            "NATURAL_DISASTER",
            "EARTHQUAKE",
            "FLOOD",
            "TYPHOON",
            "HURRICANE",
            "TSUNAMI",
            "WILDFIRE",
            "LANDSLIDE",
        ]
    ):
        return RISK_NATURAL_DISASTER

    # 軍事 / 武力衝突
    if contains_any(
        [
            "MILITARY",
            "ARMEDCONFLICT",
            "WAR",
            "INVASION",
            "MISSILE",
            "AIRSTRIKE",
            "BOMBARD",
        ]
    ):
        return RISK_MILITARY

    # 抗議活動・ストライキ
    if contains_any(
        ["PROTEST", "DEMONSTRATION", "STRIKE", "RIOT", "MOBILIZATION"]
    ):
        return RISK_CIVIL_UNREST

    # テロ / 政治暴力
    if contains_any(["TERROR", "TERRORISM", "BOMBING", "INSURGENCY"]):
        return RISK_MILITARY  # テロも広義の武力リスクとして扱う

    # 政治・選挙・政権交代
    if contains_any(
        ["ELECTION", "GOVERNMENT", "COUP", "REGIME", "PARLIAMENT", "SANCTION"]
    ):
        return RISK_POLITICAL

    # 経済・制裁・貿易
    if contains_any(["ECONOMY", "TRADE", "SANCTION", "TARIFF", "EXPORT", "IMPORT"]):
        return RISK_ECONOMIC

    return None


def classify_event_risk(cameo: str | None, theme_labels: list[str]) -> str:
    """
    イベントノード 1件を「どのリスク種別に近いか」分類する。
    - 1. CAMEO コードから推定
    - 2. Theme ラベルから補完
    - どちらも取れなければ OTHER
    """
    # 1. CAMEO ベース
    if cameo:
        by_cameo = _classify_by_cameo(cameo)
        if by_cameo is not None:
            return by_cameo

    # 2. Theme ラベルベース
    if theme_labels:
        by_theme = _classify_by_theme_labels(theme_labels)
        if by_theme is not None:
            return by_theme

    # 3. 何もわからなければ OTHER
    return RISK_OTHER


# ==== GraphML → 日付×国×リスク種別の集計 ====


def graphml_to_daily_counts(graphml_path: str) -> pd.DataFrame:
    """
    GraphML を読み込み、以下の列を持つ DataFrame を返す:
      - date: datetime64[ns]
      - country_code: str
      - risk_type: str
      - count: int
    """
    log(f"Loading GraphML: {graphml_path}")
    G = nx.read_graphml(graphml_path)
    log(f"Nodes: {G.number_of_nodes()}, Edges: {G.number_of_edges()}")

    rows: list[dict] = []

    for node_id, data in G.nodes(data=True):
        if data.get(NODE_ATTR_TYPE) != EVENT_NODE_TYPE:
            continue

        date_str = data.get(NODE_ATTR_DATE)
        if not date_str:
            continue

        try:
            event_date = datetime.strptime(str(date_str), "%Y%m%d").date()
        except Exception:
            continue

        cameo = data.get(NODE_ATTR_CAMEO)
        # 近傍の Location / Theme を収集
        countries: set[str] = set()
        theme_labels: list[str] = []

        for nbr in G.neighbors(node_id):
            nbr_data = G.nodes[nbr]
            ntype = nbr_data.get(NODE_ATTR_TYPE)

            if ntype == LOCATION_NODE_TYPE:
                cc = nbr_data.get(NODE_ATTR_COUNTRY)
                if cc:
                    countries.add(str(cc))
            elif ntype == THEME_NODE_TYPE:
                lab = nbr_data.get(NODE_ATTR_LABEL)
                if lab:
                    theme_labels.append(str(lab))

        if not countries:
            # 位置情報が取れない Event は今回は無視
            continue

        risk_type = classify_event_risk(
            cameo=str(cameo) if cameo is not None else None,
            theme_labels=theme_labels,
        )

        for cc in countries:
            rows.append(
                {
                    "date": pd.to_datetime(event_date),
                    "country_code": cc,
                    "risk_type": risk_type,
                }
            )

    if not rows:
        raise RuntimeError("Event-Location-Risk の行が 0 件でした。GraphML の構造を確認してください。")

    df = pd.DataFrame(rows)
    log(
        f"Event rows (exploded by country & risk_type): {len(df)} "
        f"(unique dates={df['date'].nunique()}, "
        f"unique countries={df['country_code'].nunique()}, "
        f"unique risk_types={df['risk_type'].nunique()})"
    )
    df_daily = (
        df.groupby(["date", "country_code", "risk_type"])
        .size()
        .reset_index(name="count")
        .sort_values(["date", "country_code", "risk_type"])
    )

    log(f"Aggregated rows: {len(df_daily)}")
    return df_daily


# ==== スパイク検知 ====


def detect_spikes(
    df_daily: pd.DataFrame,
    as_of: datetime,
    baseline_days: int,
    min_baseline_mean: float,
    min_delta_rel: float,
) -> pd.DataFrame:
    """
    df_daily（date, country_code, risk_type, count）から
    指定日のスパイクを検知し、スパイク行のみの DataFrame を返す。
    """
    # date 列を日付に揃える
    df_daily = df_daily.copy()
    df_daily["date"] = pd.to_datetime(df_daily["date"]).dt.date

    dates_min = df_daily["date"].min()
    dates_max = df_daily["date"].max()
    log(f"date range: {dates_min} .. {dates_max}")

    as_of_date = as_of.date()
    base_start = as_of_date - timedelta(days=baseline_days)
    base_end = as_of_date - timedelta(days=1)

    log(f"as_of = {as_of_date}, baseline_days = {baseline_days}")
    log(f"baseline window: {base_start} .. {base_end}")

    # 当日分
    df_today = df_daily[df_daily["date"] == as_of_date]

    # baseline 期間
    df_base = df_daily[
        (df_daily["date"] >= base_start) & (df_daily["date"] <= base_end)
    ]

    if df_today.empty:
        log("No rows for as_of date. Returning empty result.")
        return pd.DataFrame()

    # baseline が空の組は fallback: 「直近 baseline_days 日以内すべて」を baseline 扱いにする
    # country_code × risk_type 単位で集計
    # baseline_mean は fallback も含めて計算
    # 1. 当日 & baseline を集計
    agg_today = (
        df_today.groupby(["country_code", "risk_type"])["count"]
        .sum()
        .reset_index()
        .rename(columns={"count": "today"})
    )

    agg_base = (
        df_base.groupby(["country_code", "risk_type"])["count"]
        .mean()
        .reset_index()
        .rename(columns={"count": "baseline"})
    )

    # 2. left join（「今日あるところ」は必ず残す）
    merged = pd.merge(
        agg_today,
        agg_base,
        on=["country_code", "risk_type"],
        how="left",
    )

    # baseline が NaN のところは、全期間から fallback で計算
    need_fallback = merged["baseline"].isna()
    if need_fallback.any():
        log(
            f"baseline fallback needed for "
            f"{need_fallback.sum()} country_code×risk_type pairs."
        )

        df_all_before = df_daily[df_daily["date"] < as_of_date]
        fb = (
            df_all_before.groupby(["country_code", "risk_type"])["count"]
            .mean()
            .reset_index()
            .rename(columns={"count": "baseline_fb"})
        )

        merged = pd.merge(
            merged,
            fb,
            on=["country_code", "risk_type"],
            how="left",
        )

        merged["baseline"] = merged["baseline"].fillna(merged["baseline_fb"])
        merged.drop(columns=["baseline_fb"], inplace=True)

    # baseline がまだ NaN の行は除外
    merged = merged.dropna(subset=["baseline"])

    # 最低 baseline 平均を満たすものだけ残す
    before_filter = len(merged)
    merged = merged[merged["baseline"] >= min_baseline_mean]
    log(
        f"rows after baseline>= {min_baseline_mean}: "
        f"{len(merged)} (from {before_filter})"
    )

    # 増加率を計算
    merged["delta_percent"] = (merged["today"] - merged["baseline"]) / merged[
        "baseline"
    ] * 100.0

    # 閾値でフィルタ
    before_delta = len(merged)
    merged = merged[merged["delta_percent"].abs() >= min_delta_rel * 100.0]
    log(
        f"rows after |delta_percent| >= {min_delta_rel*100:.1f}%: "
        f"{len(merged)} (from {before_delta})"
    )

    # 補助列などを整える
    merged = merged.sort_values("delta_percent", ascending=False)
    merged.insert(0, "as_of", as_of_date)

    return merged.reset_index(drop=True)


# ==== CLI ====


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="GDELT GraphML から 国×リスク種別 のイベントスパイクを検知する"
    )
    parser.add_argument("graphml_path", help="入力 GraphML ファイルのパス")
    parser.add_argument(
        "--as-of",
        help="スパイク判定の対象日 (YYYY-MM-DD)。省略時は最大日付を自動検出",
        default=None,
    )
    parser.add_argument(
        "--baseline-days",
        type=int,
        default=7,
        help="baseline として使う日数（過去 N 日の平均）",
    )
    parser.add_argument(
        "--min-baseline-mean",
        type=float,
        default=1.0,
        help="baseline 平均件数の下限（これ未満の国×リスクはノイズとして無視）",
    )
    parser.add_argument(
        "--min-delta-rel",
        type=float,
        default=1.0,
        help="増加率の下限（例: 1.0 → +100%% 以上）",
    )
    parser.add_argument(
        "--output-csv",
        default="spikes_2025-11-24.csv",
        help="スパイク候補を書き出す CSV パス",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    df_daily = graphml_to_daily_counts(args.graphml_path)

    if args.as_of:
        as_of = datetime.strptime(args.as_of, "%Y-%m-%d")
    else:
        # データ中の最大日付を as_of にする
        as_of = pd.to_datetime(df_daily["date"]).max().to_pydatetime()
        log(f"--as-of 未指定のため、最大日付 {as_of.date()} を使用")

    spikes = detect_spikes(
        df_daily=df_daily,
        as_of=as_of,
        baseline_days=args.baseline_days,
        min_baseline_mean=args.min_baseline_mean,
        min_delta_rel=args.min_delta_rel,
    )

    log(f"Spike candidates: {len(spikes)}")
    if not spikes.empty:
        print(spikes.head())

    spikes.to_csv(args.output_csv, index=False)
    log(f"Saved spikes to {args.output_csv}")


if __name__ == "__main__":
    main()
