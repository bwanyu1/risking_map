"""
build_message.py

GDELT由来の「リスクスパイクイベント」から、
Discord Webhook に投げる payload(dict) を生成するモジュール。

他のコード側では、SpikeEvent を組み立てて
    payload = build_discord_payload(event)
    requests.post(WEBHOOK_URL, json=payload)
のように使うことを想定。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


# ---------- データ構造定義 ----------

@dataclass
class Article:
    """根拠となるニュース記事1本分の情報"""
    title: str
    url: str
    source: Optional[str] = None  # "Reuters", "Bloomberg" など任意


@dataclass
class SimilarCase:
    """過去の類似イベントの情報（あれば）"""
    date: str  # "2022-08-01" など文字列でOK
    description: str  # "ペロシ訪台" など
    market_reaction: str  # "TSMC -5.2%, SOXX -3.8%" など


@dataclass
class SpikeEvent:
    """
    Discord に通知したい「リスクスパイク」1件分の情報。
    GDELT 集計や ΔR 計算の結果から、他モジュール側で組み立てる。
    """
    country_name: str           # "台湾", "中国", "米国" など
    risk_type: str              # "軍事", "エネルギー", "政策", "災害" など
    delta_percent: float        # ΔR (%) 例: 350.0
    abs_count: int              # 今日の件数
    baseline: float             # 過去7日平均
    main_themes: List[str]      # ["CHINA_MILITARY", ...]
    assets: List[str]           # 影響が出そうなアセット（銘柄・ETF・通貨など）

    level: str = "alert"        # "info" | "warning" | "alert" | "critical"
    confidence: Optional[str] = None  # "高", "中", "低" など任意

    similar_cases: List[SimilarCase] = field(default_factory=list)
    articles: List[Article] = field(default_factory=list)


# ---------- 内部ヘルパー ----------

def _level_to_emoji_and_color(level: str) -> (str, int):
    """
    アラートレベルから絵文字とDiscord埋め込みカラーを決定。
    color は 10 進数の整数（0xRRGGBB）。
    """
    level = level.lower()
    if level == "critical":
        return "🛑", 0xE74C3C  # 赤
    if level == "alert":
        return "🔥", 0xE67E22  # オレンジ
    if level == "warning":
        return "⚠️", 0xF1C40F  # 黄
    # デフォルト（info）
    return "ℹ️", 0x3498DB      # 青


def _format_main_themes(themes: List[str]) -> str:
    if not themes:
        return "（テーマ情報なし）"
    return "\n".join(f"- {t}" for t in themes)


def _format_assets(assets: List[str]) -> str:
    if not assets:
        return "（影響が想定されるアセットは未推定）"
    return "\n".join(f"- {a}" for a in assets)


def _format_similar_cases(cases: List[SimilarCase]) -> str:
    if not cases:
        return "類似ケースの記録はまだありません。"
    lines = []
    for c in cases:
        lines.append(f"{c.date} {c.description} → {c.market_reaction}")
    return "\n".join(lines)


def _format_articles(articles: List[Article], confidence: Optional[str]) -> str:
    if not articles:
        return "（根拠記事の取得に失敗 or まだ実装されていません）"

    lines = []
    for i, a in enumerate(articles, start=1):
        src = f"{a.source}：" if a.source else ""
        # Discord では [text](url) 形式でリンク可能
        lines.append(f"{i}. {src}[{a.title}]({a.url})")
    conf = f"信頼度: {confidence}" if confidence else "信頼度: （未設定）"
    return conf + "\n" + "\n".join(lines)


# ---------- 公開関数：Discord Webhook Payload 生成 ----------

def build_discord_payload(event: SpikeEvent) -> Dict[str, Any]:
    """
    SpikeEvent から Discord Webhook 用の payload(dict) を生成する。

    戻り値はそのまま
        requests.post(WEBHOOK_URL, json=payload)
    に渡せる形。
    """
    emoji, color = _level_to_emoji_and_color(event.level)

    # タイトル例: "🔥 台湾：軍事リスク +350%"
    title = f"{emoji} {event.country_name}：{event.risk_type}リスク {event.delta_percent:+.0f}%"

    # 説明文（件数とベースラインを軽く表示）
    description = (
        f"過去7日平均と比較してニュース量が急増しています。\n"
        f"- 本日の件数: {event.abs_count}\n"
        f"- 過去7日平均: {event.baseline:.1f}\n"
    )

    # 埋め込みフィールドを構築
    fields = [
        {
            "name": "📍 主因テーマ",
            "value": _format_main_themes(event.main_themes),
            "inline": False,
        },
        {
            "name": "🎯 影響が予想されるアセット",
            "value": _format_assets(event.assets),
            "inline": False,
        },
        {
            "name": "📚 過去の類似ケース",
            "value": _format_similar_cases(event.similar_cases),
            "inline": False,
        },
        {
            "name": "📰 根拠記事",
            "value": _format_articles(event.articles, event.confidence),
            "inline": False,
        },
    ]

    embed = {
        "title": title,
        "description": description,
        "color": color,
        "fields": fields,
    }

    payload: Dict[str, Any] = {
        "content": None,  # テキスト本文はいったん無し。必要ならここに @everyone など
        "embeds": [embed],
    }
    return payload


# ---------- お試し用の簡単サンプル ----------

if __name__ == "__main__":
    # 手動テスト用（実際の送信は別スクリプトで）
    sample_event = SpikeEvent(
        country_name="台湾",
        risk_type="軍事",
        delta_percent=350.0,
        abs_count=120,
        baseline=30.5,
        main_themes=["CHINA_MILITARY", "AIRSPACE_VIOLATION"],
        assets=[
            "TSMC (2330.TW)",
            "NVDA",
            "SOXX（半導体ETF）",
            "台湾ドル (TWD)",
        ],
        level="alert",
        confidence="高",
        similar_cases=[
            SimilarCase(
                date="2022-08-02",
                description="ペロシ米下院議長の台湾訪問",
                market_reaction="TSMC -5.2%, SOXX -3.8%, VIX +18%",
            )
        ],
        articles=[
            Article(
                title="China launches military drills around Taiwan",
                url="https://example.com/reuters1",
                source="Reuters",
            ),
            Article(
                title="Tensions rise in Taiwan Strait",
                url="https://example.com/bloomberg1",
                source="Bloomberg",
            ),
        ],
    )

    import json
    print(json.dumps(build_discord_payload(sample_event), ensure_ascii=False, indent=2))
