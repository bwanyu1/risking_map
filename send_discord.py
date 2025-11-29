# send_discord.py

from __future__ import annotations

import os
import time
from typing import Iterable, List, Optional

import requests

from build_message import SpikeEvent, build_discord_payload


class DiscordSenderError(Exception):
    """Discord送信まわりのエラー用"""

    pass


def send_discord_payload(
    webhook_url: str,
    payload: dict,
    *,
    timeout: int = 10,
    max_retries: int = 3,
    retry_backoff_sec: float = 2.0,
) -> None:
    """
    Discord Webhook に payload(dict) を POST する低レイヤ関数。
    単発の embed 送信にも使える。

    :param webhook_url: Discord Webhook の URL
    :param payload: build_discord_payload で作った dict
    :param timeout: リクエストのタイムアウト秒
    :param max_retries: 失敗時の最大リトライ回数
    :param retry_backoff_sec: リトライごとの待機秒 (指数バックオフベース)
    """
    last_err: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(webhook_url, json=payload, timeout=timeout)
            if resp.status_code == 204:
                # Discord Webhookは成功時 204 No Content を返す
                return
            # レートリミット対応 (429)
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        sleep_sec = float(retry_after)
                    except ValueError:
                        sleep_sec = retry_backoff_sec * attempt
                else:
                    sleep_sec = retry_backoff_sec * attempt

                print(f"[Discord] Rate limited. sleep {sleep_sec:.1f}s")
                time.sleep(sleep_sec)
                continue

            # その他のエラー
            raise DiscordSenderError(
                f"Discord webhook error: {resp.status_code} {resp.text}"
            )
        except Exception as e:  # requests 例外もまとめて扱う
            last_err = e
            if attempt == max_retries:
                break
            sleep_sec = retry_backoff_sec * attempt
            print(f"[Discord] Error on attempt {attempt}/{max_retries}: {e}")
            print(f"[Discord] Retry after {sleep_sec:.1f}s ...")
            time.sleep(sleep_sec)

    # ここまで来たら失敗
    raise DiscordSenderError(f"Failed to send Discord webhook: {last_err}")


def send_spike_event(
    event: SpikeEvent,
    webhook_url: str,
    *,
    dry_run: bool = False,
) -> None:
    """
    単一の SpikeEvent を Discord に送信するユーティリティ。

    :param event: SpikeEvent インスタンス
    :param webhook_url: Discord Webhook URL
    :param dry_run: True のときは送信せず payload だけ print
    """
    payload = build_discord_payload(event)

    if dry_run:
        import json

        print("[Discord dry-run] payload:")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    send_discord_payload(webhook_url, payload)


def send_spike_events_batch(
    events: Iterable[SpikeEvent],
    webhook_url: str,
    *,
    max_events: int = 5,
    dry_run: bool = False,
    sleep_between_sec: float = 1.0,
) -> List[SpikeEvent]:
    """
    SpikeEvent のリストをまとめて送信する。
    ・上位 max_events 件だけ送る（スパム防止）
    ・送信に成功したものをリストで返す

    :param events: SpikeEvent iterable
    :param webhook_url: Discord Webhook URL
    :param max_events: 1回の実行で送信する最大件数
    :param dry_run: True のときは送信せず payload を標準出力
    :param sleep_between_sec: 複数送るときのインターバル秒
    :return: 送信したイベントのリスト
    """
    sent: List[SpikeEvent] = []
    for i, ev in enumerate(events):
        if i >= max_events:
            print(f"[Discord] Max events ({max_events}) reached. Skipping remaining.")
            break

        print(
            f"[Discord] Sending event {i+1}/{max_events}: "
            f"{ev.country_name} {ev.risk_type} ΔR={ev.delta_percent:.0f}%"
        )

        send_spike_event(ev, webhook_url, dry_run=dry_run)
        sent.append(ev)

        if not dry_run and sleep_between_sec > 0 and i < max_events - 1:
            time.sleep(sleep_between_sec)

    return sent


# ちょっとした手動テスト用
if __name__ == "__main__":
    from build_message import SimilarCase, Article

    webhook = "https://discordapp.com/api/webhooks/1444247395306180784/SMjZhwjZyw0rypjrgjZY69QNJIHM35r08PfpQmTzmCpL5r8tBPb9Inbn0pDik-XLLa6Y"
    if not webhook:
        raise RuntimeError("環境変数 DISCORD_WEBHOOK_URL を設定してください。")

    dummy = SpikeEvent(
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
            )
        ],
    )

    # 最初は dry_run=True で payload 確認してから False にすると安心
    send_spike_event(dummy, webhook, dry_run=False)
    print("done.")
