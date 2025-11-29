#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Literal

import pandas as pd

Severity = Literal["LOW", "MEDIUM", "HIGH", "EXTREME"]


@dataclass
class SpikeEvent:
    """
    GDELT 集計結果から作る「リスクスパイク」１件分の構造。
    """
    date: str              # "YYYY-MM-DD" （as_of）
    country_code: str      # 2文字国コード（GDELT の country_code）
    risk_type: str         # "ECONOMIC" / "POLITICAL" / "MILITARY" / "CIVIL_UNREST" / "OTHER"
    today_count: int       # 今日のイベント件数
    baseline_mean: float   # 平常時の平均件数
    delta_percent: float   # 変化率（%）
    severity: Severity     # LOW / MEDIUM / HIGH / EXTREME

    @property
    def id(self) -> str:
        """ユニークID（ログやキャッシュ用）"""
        return f"{self.date}:{self.country_code}:{self.risk_type}"


# -----------------------------
# しきい値・セグメント定義
# -----------------------------

DEFAULT_MIN_TODAY = 10          # 今日の件数が少なすぎるものは捨てる
DEFAULT_MIN_BASELINE = 1.0      # ベースライン 0〜小さすぎるものはノイズ扱い
DEFAULT_MIN_DELTA_PERCENT = 100 # +100% 未満は「スパイク」と呼ばない

def classify_severity(delta_percent: float) -> Severity:
    """
    変化率からシビア度を分類。
    必要になったらここだけ変えれば通知の粒度を調整できる。
    """
    if delta_percent >= 1000:
        return "EXTREME"
    if delta_percent >= 500:
        return "HIGH"
    if delta_percent >= 200:
        return "MEDIUM"
    return "LOW"


# -----------------------------
# CSV → SpikeEvent 変換本体
# -----------------------------

REQUIRED_COLUMNS = {
    "as_of",
    "country_code",
    "risk_type",
    "today",
    "baseline",
    "delta_percent",
}


def load_spike_events_from_csv(
    csv_path: str | Path,
    min_today: int = DEFAULT_MIN_TODAY,
    min_baseline: float = DEFAULT_MIN_BASELINE,
    min_delta_percent: float = DEFAULT_MIN_DELTA_PERCENT,
) -> List[SpikeEvent]:
    """
    集計済み CSV（as_of / country_code / risk_type / today / baseline / delta_percent）
    を読み込み、SpikeEvent のリストに変換する。
    """
    csv_path = Path(csv_path)
    df = pd.read_csv(csv_path)

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"CSV に必要な列がありません: {missing}")

    # しきい値フィルタ
    mask = (
        (df["today"] >= min_today)
        & (df["baseline"] >= min_baseline)
        & (df["delta_percent"] >= min_delta_percent)
    )
    df = df.loc[mask].copy()

    # 変化率の大きい順にソート（上から通知する想定）
    df = df.sort_values("delta_percent", ascending=False)

    events: List[SpikeEvent] = []
    for _, row in df.iterrows():
        sev = classify_severity(float(row["delta_percent"]))
        ev = SpikeEvent(
            date=str(row["as_of"]),               # 既に "YYYY-MM-DD" 想定
            country_code=str(row["country_code"]),
            risk_type=str(row["risk_type"]),
            today_count=int(row["today"]),
            baseline_mean=float(row["baseline"]),
            delta_percent=float(row["delta_percent"]),
            severity=sev,
        )
        events.append(ev)

    return events


# -----------------------------
# CLI（JSON Lines で吐き出す）
# -----------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="GDELT 集計 CSV → SpikeEvent(JSONL) 変換ツール"
    )
    parser.add_argument(
        "csv_path",
        help="graphml_to_spikes.py などで出力した集計 CSV のパス",
    )
    parser.add_argument(
        "--min-today",
        type=int,
        default=DEFAULT_MIN_TODAY,
        help=f"今日の件数の下限（デフォルト: {DEFAULT_MIN_TODAY}）",
    )
    parser.add_argument(
        "--min-baseline",
        type=float,
        default=DEFAULT_MIN_BASELINE,
        help=f"ベースライン平均件数の下限（デフォルト: {DEFAULT_MIN_BASELINE}）",
    )
    parser.add_argument(
        "--min-delta-percent",
        type=float,
        default=DEFAULT_MIN_DELTA_PERCENT,
        help=f"変化率の下限（デフォルト: {DEFAULT_MIN_DELTA_PERCENT}%%）",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="デバッグ用：JSON を整形して標準出力に出す（人間向け）",
    )

    args = parser.parse_args()

    events = load_spike_events_from_csv(
        csv_path=args.csv_path,
        min_today=args.min_today,
        min_baseline=args.min_baseline,
        min_delta_percent=args.min_delta_percent,
    )

    for ev in events:
        obj = asdict(ev)
        if args.pretty:
            print(json.dumps(obj, ensure_ascii=False, indent=2))
        else:
            # JSON Lines 形式（1行1イベント）で吐く
            print(json.dumps(obj, ensure_ascii=False))


if __name__ == "__main__":
    main()
