#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from typing import List, Dict, Any

import requests


def load_spikes(jsonl_path: str) -> list[dict]:
    """
    JSONL を読み込んで SpikeEvent の list を返す。
    - 複数の文字コードを順番に試す
    - 各行ごとに json.loads を試し、失敗した行はスキップ
    """
    encodings = [
        "utf-8",
        "utf-8-sig",
        "utf-16",
        "utf-16-le",
        "utf-16-be",
        "cp932",
        "shift_jis",
        "latin-1",
    ]
    last_error: Exception | None = None

    for enc in encodings:
        events: list[dict] = []
        try:
            print(f"[load_spikes] try encoding = {enc}")
            with open(jsonl_path, "r", encoding=enc) as f:
                for lineno, line in enumerate(f, start=1):
                    raw = line.strip()
                    if not raw:
                        continue
                    # 先頭の BOM を落とす
                    raw = raw.lstrip("\ufeff")
                    try:
                        obj = json.loads(raw)
                    except json.JSONDecodeError as e:
                        print(f"[load_spikes] skip line {lineno} ({enc}): {e}")
                        continue
                    events.append(obj)

            if events:
                print(f"[load_spikes] encoding {enc} で {len(events)} 件ロード")
                return events
            else:
                last_error = RuntimeError(f"encoding {enc} では有効な JSON 行が 0 件")
        except Exception as e:
            last_error = e
            continue

    raise RuntimeError(f"JSONL を開けませんでした。最終エラー: {last_error}")

def format_discord_message(ev: Dict[str, Any]) -> str:
    """
    SpikeEvent → Discord メッセージ文字列
    （gdelt_spike_to_events.py の出力スキーマに合わせている）
    """
    date = ev.get("date")
    country_code = ev.get("country_code", "??")
    risk_type = ev.get("risk_type", "ALL")
    today = ev.get("today_count")
    baseline = ev.get("baseline_mean")
    delta = ev.get("delta_percent")
    severity = ev.get("severity", "LOW")

    # severity に応じてちょっと表現を変える
    severity_emoji = {
        "EXTREME": "🟥",
        "HIGH": "🟧",
        "MEDIUM": "🟨",
        "LOW": "🟩",
    }.get(severity, "🟩")

    header_emoji = "🔥" if severity in ("HIGH", "EXTREME") else "⚠️"

    lines = []
    lines.append(f"{header_emoji} 地政学リスク急増検知 {header_emoji}")
    lines.append(f"レベル: {severity_emoji} **{severity}**")
    lines.append(f"日付: **{date}**")
    lines.append(f"国: **{country_code}**")
    lines.append(f"リスク種別: **{risk_type}**")
    lines.append("")
    lines.append(f"・今日の報道量: **{today} 件**")
    lines.append(f"・平常時平均: **{baseline} 件**")
    lines.append(f"・異常度: **{round(delta, 1)}% 増加**")

    return "\n".join(lines)


def send_to_discord(webhook_url: str, content: str) -> None:
    """Discord にメッセージを投げる"""
    payload = {"content": content}
    r = requests.post(webhook_url, json=payload)
    try:
        r.raise_for_status()
    except Exception as e:
        # デバッグしやすいようにレスポンス本文も出す
        print("[notify] Discord error:", e, "status:", r.status_code, "body:", r.text)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SpikeEvent(JSONL) を Discord Webhook に投げる簡易ツール"
    )
    parser.add_argument("jsonl_path", help="gdelt_spike_to_events.py で生成した JSONL")
    parser.add_argument("--webhook", required=True, help="Discord Webhook URL")
    parser.add_argument(
        "--max-events",
        type=int,
        default=10,
        help="一度に通知する最大件数（デフォルト: 10）",
    )
    args = parser.parse_args()

    spikes = load_spikes(args.jsonl_path)

    if not spikes:
        print("[notify] SpikeEvent は 0 件でした。通知しません。")
        return

    # 念のため delta_percent の降順に
    spikes_sorted = sorted(
        spikes, key=lambda x: float(x.get("delta_percent", 0.0)), reverse=True
    )

    to_send = spikes_sorted[: args.max_events]
    print(f"[notify] {len(spikes)} 件中 {len(to_send)} 件を Discord に通知します。")

    for ev in to_send:
        msg = format_discord_message(ev)
        print("[notify] Sending:", ev.get("date"), ev.get("country_code"), ev.get("delta_percent"))
        send_to_discord(args.webhook, msg)


if __name__ == "__main__":
    main()
