# run_daily_from_graphml.py (例)

from datetime import datetime
import os

from graphml_to_spikes import graphml_to_daily_counts_from_events, daily_counts_to_spike_df
from gdelt_to_spike import df_to_spike_events   # 前に作ったやつ
from send_discord import send_spike_events_batch       # 前に作ったやつ


GRAPHML_PATH = "gdelt_full_kg.graphml"
WEBHOOK_URL = os.environ["DISCORD_WEBHOOK_URL"]


def main():
    df_daily = graphml_to_daily_counts_from_events(GRAPHML_PATH)
    df_spikes = daily_counts_to_spike_df(df_daily)

    if df_spikes.empty:
        print("[run_daily] No spike detected.")
        return

    events = df_to_spike_events(df_spikes)
    send_spike_events_batch(events, WEBHOOK_URL, max_events=3, dry_run=False)


if __name__ == "__main__":
    main()
