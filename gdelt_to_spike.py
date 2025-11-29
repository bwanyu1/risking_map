"""
gdelt_to_spike.py

GDELT から集計した「国×リスク種別ごとのスパイク情報」を
build_message.SpikeEvent に変換するためのユーティリティ。

想定フロー：
 1. 別モジュールで GDELT GKG を集計して DataFrame を作る
 2. 本モジュールの df_to_spike_events() に渡す
 3. 返ってきた SpikeEvent を Discord Webhook に流す
"""

from __future__ import annotations

from typing import Dict, List, Any, Optional, Tuple

import pandas as pd

from build_message import (
    SpikeEvent,
    SimilarCase,
    Article,
)


# ========== 1. アセット推定ルールの例 ==========

# シンプルなルールベース例
# （本番ではここを別ファイル or DB に出してもOK）
DEFAULT_ASSET_RULES: Dict[Tuple[str, str], List[str]] = {
    # (country_name, risk_type) : [asset, ...]
    ("台湾", "軍事"): [
        "TSMC (2330.TW)",
        "SOXX（半導体ETF）",
        "NVDA",
        "AMD",
        "台湾ドル (TWD)",
        "金 (GOLD)",
    ],
    ("中東", "エネルギー"): [
        "WTI 原油先物",
        "BRENT 原油先物",
        "XLE（エネルギーETF）",
        "サウジ関連株",
    ],
    ("ウクライナ", "軍事"): [
        "小麦先物",
        "エネルギー銘柄",
        "欧州株式インデックス",
    ],
}

# country_name が合致しなくても、risk_type だけで使うデフォルトルール
DEFAULT_ASSET_RULES_BY_RISK: Dict[str, List[str]] = {
    "軍事": [
        "ディフェンス関連株（LMT, RTX など）",
        "エネルギーETF",
        "金 (GOLD)",
        "VIX（恐怖指数）",
    ],
    "エネルギー": [
        "WTI 原油先物",
        "BRENT 原油先物",
        "XLE（エネルギーETF）",
    ],
    "災害": [
        "保険セクターETF",
        "コモディティ",
    ],
}


# ========== 2. レベル・信頼度の自動推定 ==========

def infer_level(delta_percent: float) -> str:
    """
    ΔR[%] からアラートレベルをざっくり決める。
      400%以上: critical
      250%以上: alert
      150%以上: warning
      それ未満 : info
    """
    if delta_percent >= 400:
        return "critical"
    if delta_percent >= 250:
        return "alert"
    if delta_percent >= 150:
        return "warning"
    return "info"


def infer_confidence(
    delta_percent: float,
    abs_count: int,
    baseline: float,
    source_count: Optional[int] = None,
) -> str:
    """
    簡易な「信頼度」推定。
    - 件数が少なすぎる、あるいは baseline が極端に小さい場合は「低」
    - ソース数が多い・件数もそこそこなら「高」
    """
    # 極端にデータが薄い場合
    if abs_count < 5 or baseline < 1:
        return "低"

    # ソース数が取れていれば重視（仮）
    if source_count is not None:
        if source_count >= 10 and delta_percent >= 200:
            return "高"
        if source_count >= 5:
            return "中"
        return "低"

    # ソース数なしの場合は ΔR と件数ベースで判断
    if delta_percent >= 300 and abs_count >= 30:
        return "高"
    if delta_percent >= 150 and abs_count >= 10:
        return "中"
    return "低"


# ========== 3. アセット・類似ケース・記事の紐づけ ==========

def guess_assets(
    country_name: str,
    risk_type: str,
    asset_rules_exact: Optional[Dict[Tuple[str, str], List[str]]] = None,
    asset_rules_by_risk: Optional[Dict[str, List[str]]] = None,
) -> List[str]:
    """
    国名とリスク種別から、影響が出そうなアセットを推定する。
    ルールが見つからなければ空リストを返す。
    """
    asset_rules_exact = asset_rules_exact or DEFAULT_ASSET_RULES
    asset_rules_by_risk = asset_rules_by_risk or DEFAULT_ASSET_RULES_BY_RISK

    key = (country_name, risk_type)
    if key in asset_rules_exact:
        return asset_rules_exact[key]

    if risk_type in asset_rules_by_risk:
        return asset_rules_by_risk[risk_type]

    return []


def lookup_similar_cases(
    country_name: str,
    risk_type: str,
    similar_cases_index: Optional[
        Dict[Tuple[str, str], List[SimilarCase]]
    ] = None,
) -> List[SimilarCase]:
    """
    簡易な「過去類似ケース」参照。
    - v1 では None or 空 dict の想定でもOK
    - 将来: 実測データから埋める
    """
    if not similar_cases_index:
        return []
    key = (country_name, risk_type)
    return similar_cases_index.get(key, [])


def lookup_articles(
    row_key: Any,
    articles_index: Optional[Dict[Any, List[Article]]] = None,
) -> List[Article]:
    """
    事前に作成しておいた「行単位の関連記事リスト」から取得。
    row_key は (country, risk_type) や row_id など任意。
    """
    if not articles_index:
        return []
    return articles_index.get(row_key, [])


# ========== 4. DataFrame → SpikeEvent 変換の本体 ==========

def df_to_spike_events(
    df_spikes: pd.DataFrame,
    *,
    asset_rules_exact: Optional[Dict[Tuple[str, str], List[str]]] = None,
    asset_rules_by_risk: Optional[Dict[str, List[str]]] = None,
    similar_cases_index: Optional[
        Dict[Tuple[str, str], List[SimilarCase]]
    ] = None,
    articles_index: Optional[Dict[Any, List[Article]]] = None,
) -> List[SpikeEvent]:
    """
    GDELT 集計済みの DataFrame から SpikeEvent のリストを生成する。

    想定カラム:
      - country_name: str   ... "台湾", "中国" など（必須）
      - risk_type:   str   ... "軍事", "エネルギー" など（必須）
      - delta_percent: float ... ΔR（%）（必須）
      - abs_count:   int   ... 今日の件数（必須）
      - baseline:    float ... 過去7日平均（必須）
      - main_themes: list[str] または "AAA;BBB" などの文字列（任意）
      - source_count: int   ... 記事ソース数（任意）

    それ以外の情報（類似ケース・記事）はオプション引数で外付けする。
    """
    events: List[SpikeEvent] = []

    for _, row in df_spikes.iterrows():
        country_name = str(row["country_name"])
        risk_type = str(row["risk_type"])
        delta_percent = float(row["delta_percent"])
        abs_count = int(row["abs_count"])
        baseline = float(row["baseline"])

        # main_themes が文字列だったら簡易パース
        main_themes_raw = row.get("main_themes", [])
        if isinstance(main_themes_raw, str):
            # ";" 区切り / "," 区切りなどを想定
            parts = [p.strip() for p in main_themes_raw.replace(",", ";").split(";")]
            main_themes: List[str] = [p for p in parts if p]
        elif isinstance(main_themes_raw, list):
            main_themes = main_themes_raw
        else:
            main_themes = []

        source_count = None
        if "source_count" in row and not pd.isna(row["source_count"]):
            source_count = int(row["source_count"])

        # レベル & 信頼度
        level = infer_level(delta_percent)
        confidence = infer_confidence(
            delta_percent=delta_percent,
            abs_count=abs_count,
            baseline=baseline,
            source_count=source_count,
        )

        # アセット推定
        assets = guess_assets(
            country_name=country_name,
            risk_type=risk_type,
            asset_rules_exact=asset_rules_exact,
            asset_rules_by_risk=asset_rules_by_risk,
        )

        # 類似ケース
        similar_cases = lookup_similar_cases(
            country_name=country_name,
            risk_type=risk_type,
            similar_cases_index=similar_cases_index,
        )

        # 記事リスト
        # row_key はとりあえず (country_name, risk_type) にしておくが、
        # 行IDなどに変える場合は呼び出し側で合わせればよい。
        row_key = (country_name, risk_type)
        articles = lookup_articles(
            row_key=row_key,
            articles_index=articles_index,
        )

        event = SpikeEvent(
            country_name=country_name,
            risk_type=risk_type,
            delta_percent=delta_percent,
            abs_count=abs_count,
            baseline=baseline,
            main_themes=main_themes,
            assets=assets,
            level=level,
            confidence=confidence,
            similar_cases=similar_cases,
            articles=articles,
        )
        events.append(event)

    return events


# ========== 5. 簡単なお試しコード ==========

if __name__ == "__main__":
    # ダミーの DataFrame を作ってテスト
    data = [
        {
            "country_name": "台湾",
            "risk_type": "軍事",
            "delta_percent": 350.0,
            "abs_count": 120,
            "baseline": 30.5,
            "main_themes": "CHINA_MILITARY;AIRSPACE_VIOLATION",
            "source_count": 12,
        },
        {
            "country_name": "中東",
            "risk_type": "エネルギー",
            "delta_percent": 220.0,
            "abs_count": 80,
            "baseline": 25.0,
            "main_themes": ["OIL_SUPPLY", "PIPELINE_ATTACK"],
            "source_count": 8,
        },
    ]
    df = pd.DataFrame(data)

    events = df_to_spike_events(df)
    for ev in events:
        print(ev)
