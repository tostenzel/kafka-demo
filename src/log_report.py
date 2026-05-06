"""Human-readable report for producer/consumer demo logs."""

import json
from collections import Counter

from rich.console import Console
from rich.table import Table
from rich import box
from rich.text import Text

from src.helpers import PRODUCER_LOG, read_processed_log

console = Console()


def _load_produced() -> list[dict]:
    with open(PRODUCER_LOG, encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    produced = _load_produced()
    processed = read_processed_log()

    produced_by_key = {e["key"]: e for e in produced}
    processed_key_set = {e["key"] for e in processed if e.get("key")}

    missing = [e for e in produced if e["key"] not in processed_key_set]
    offsets = [(e["partition"], e["offset"]) for e in processed if "partition" in e]
    offset_counts = Counter(offsets)
    duplicates = [(po, count) for po, count in offset_counts.items() if count > 1]
    n_duplicates = sum(count - 1 for _, count in duplicates)

    # ── summary ──────────────────────────────────────────────────────────────
    summary = Table(box=box.SIMPLE, show_header=False, pad_edge=False)
    summary.add_column(style="bold")
    summary.add_column(justify="right")
    summary.add_row("Produced", str(len(produced)))
    summary.add_row("Processed", str(len(processed)))
    summary.add_row(
        "Lost",
        Text(str(len(missing)), style="bold red" if missing else "green"),
    )
    summary.add_row(
        "Duplicates",
        Text(str(n_duplicates), style="bold yellow" if n_duplicates else "green"),
    )
    console.rule("[bold]Log summary")
    console.print(summary)

    # ── lost messages ─────────────────────────────────────────────────────────
    if missing:
        console.rule("[bold red]Lost messages")
        for e in missing:
            console.print(f"  [bold red]{e['key']}[/]  {e['value']}")
        console.print()

    # ── duplicates ───────────────────────────────────────────────────────────
    if duplicates:
        console.rule("[bold yellow]Duplicate offsets")
        for (partition, offset), count in duplicates:
            console.print(f"  partition={partition} offset={offset} seen=[bold yellow]{count}x[/]")
        console.print()

    # ── processed entries ────────────────────────────────────────────────────
    console.rule("[bold]Processed entries")
    table = Table(box=box.SIMPLE, show_edge=False)
    table.add_column("consumer", style="cyan", no_wrap=True)
    table.add_column("partition", justify="right", style="dim")
    table.add_column("offset", justify="right", style="dim")
    table.add_column("key", style="bold")
    table.add_column("value")

    for e in processed:
        table.add_row(
            e["consumer"],
            str(e["partition"]),
            str(e["offset"]),
            e.get("key", ""),
            e.get("value", ""),
        )

    console.print(table)


if __name__ == "__main__":
    main()
