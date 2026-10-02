"""Draw docs/img/architecture-{light,dark}.svg."""

from pathlib import Path

OUT = Path(__file__).parent / "img"
THEMES = {
    "light": {
        "bg": "#fcfcfb",
        "card": "#ffffff",
        "line": "#c3c2b7",
        "text": "#0b0b0b",
        "text2": "#52514e",
        "muted": "#898781",
        "accent": "#2a78d6",
        "tint": "#eef4fc",
        "live": "#eb6834",
        "livetint": "#fdf0ea",
    },
    "dark": {
        "bg": "#1a1a19",
        "card": "#232322",
        "line": "#383835",
        "text": "#ffffff",
        "text2": "#c3c2b7",
        "muted": "#898781",
        "accent": "#3987e5",
        "tint": "#1d2a3a",
        "live": "#d95926",
        "livetint": "#33231b",
    },
}
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"


def box(x, y, w, h, title, lines, t, *, fill=None, stroke=None, title_color=None):
    out = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{fill or t["card"]}" '
        f'stroke="{stroke or t["line"]}" stroke-width="1.2"/>',
        f'<text x="{x + 14}" y="{y + 24}" font-size="13.5" font-weight="600" '
        f'fill="{title_color or t["text"]}">{title}</text>',
    ]
    for i, line in enumerate(lines):
        out.append(
            f'<text x="{x + 14}" y="{y + 46 + i * 18}" font-size="12" '
            f'fill="{t["text2"]}">{line}</text>'
        )
    return out


def arrow(x1, y1, x2, y2, t, color=None):
    c = color or t["muted"]
    return [
        f'<line x1="{x1}" y1="{y1}" x2="{x2 - 6}" y2="{y2}" stroke="{c}" stroke-width="1.5"/>',
        f'<path d="M{x2 - 7},{y2 - 4.5} L{x2},{y2} L{x2 - 7},{y2 + 4.5} Z" fill="{c}"/>',
    ]


def label(x, y, text, t, *, size=11, color=None, anchor="start", weight="400"):
    return [
        f'<text x="{x}" y="{y}" font-size="{size}" fill="{color or t["muted"]}" '
        f'text-anchor="{anchor}" font-weight="{weight}">{text}</text>'
    ]


def draw(theme: str) -> str:
    t = THEMES[theme]
    W, H = 1120, 520
    s = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" '
        f'height="{H}" font-family="{FONT}">',
        f'<rect width="{W}" height="{H}" rx="14" fill="{t["bg"]}"/>',
    ]
    cols = {"src": 24, "load": 246, "check": 444, "lake": 664, "use": 916}
    for key, text in [
        ("src", "SOURCES"),
        ("load", "LOADERS"),
        ("check", "EVERY LOAD"),
        ("lake", "DUCKLAKE"),
        ("use", "USED BY"),
    ]:
        s += label(cols[key], 34, text, t, size=11, weight="600")

    # sources, 196 wide
    batch = [
        (
            "Monthly files",
            ["TLC trips: Uber, Lyft, taxis", "Citi Bike trips", "boundaries, GTFS schedule"],
        ),
        ("NYC and NY open data", ["311 requests, crashes", "subway ridership", "road speeds"]),
        ("Weather", ["Open-Meteo hourly", "Weather Service alerts"]),
    ]
    y, mids = 50, []
    for title, lines in batch:
        h = 34 + 18 * len(lines)
        s += box(cols["src"], y, 196, h, title, lines, t)
        mids.append(y + h / 2)
        y += h + 14
    live_y = y + 10
    s += box(
        cols["src"],
        live_y,
        196,
        70,
        "Live feeds",
        ["subway GTFS-realtime, 30 s", "Citi Bike stations, 1 min"],
        t,
        fill=t["livetint"],
        stroke=t["live"],
    )

    # loaders, 170 wide
    s += box(
        cols["load"],
        98,
        172,
        140,
        "Batch loads",
        [
            "one systemd timer",
            "per source",
            "retries, backoff,",
            "circuit breaker,",
            "conditional GET",
        ],
        t,
    )
    s += box(
        cols["load"],
        live_y,
        172,
        70,
        "Live pollers",
        ["train tracker,", "change-only stations"],
        t,
        fill=t["livetint"],
        stroke=t["live"],
    )
    for i, m in enumerate(mids):
        s += arrow(cols["src"] + 196, m, cols["load"], 140 + i * 24, t)
    s += arrow(cols["src"] + 196, live_y + 35, cols["load"], live_y + 35, t, color=t["live"])

    # checks and writes, 196 wide
    s += box(
        cols["check"],
        98,
        196,
        122,
        "Contract checks",
        ["types, keys, ranges", "bad rows to quarantine", "batch stops above", "a threshold"],
        t,
    )
    s += box(
        cols["check"],
        258,
        196,
        104,
        "Idempotent writes",
        ["replace, merge, append-new", "one snapshot per load", "checkpoint after commit"],
        t,
    )
    s += box(
        cols["check"],
        384,
        196,
        70,
        "Never stored",
        ["raw files are deleted", "once a load commits"],
        t,
    )
    s += arrow(cols["load"] + 172, 168, cols["check"], 160, t)
    s += [
        f'<polyline points="{cols["load"] + 172},{live_y + 35} 430,{live_y + 35} 430,190 '
        f'{cols["check"] - 6},190" fill="none" stroke="{t["live"]}" stroke-width="1.5"/>',
        f'<path d="M{cols["check"] - 7},185.5 L{cols["check"]},190 L{cols["check"] - 7},194.5 Z" '
        f'fill="{t["live"]}"/>',
    ]
    x_mid = cols["check"] + 98
    s += [
        f'<line x1="{x_mid}" y1="220" x2="{x_mid}" y2="251" stroke="{t["muted"]}" stroke-width="1.5"/>',
        f'<path d="M{x_mid - 4.5},250 L{x_mid},258 L{x_mid + 4.5},250 Z" fill="{t["muted"]}"/>',
    ]

    # lake, 222 wide
    lx = cols["lake"]
    s += [
        f'<rect x="{lx}" y="50" width="222" height="404" rx="12" fill="{t["tint"]}" '
        f'stroke="{t["accent"]}" stroke-width="1.2"/>'
    ]
    s += label(lx + 14, 72, "Parquet on disk, catalog in Postgres", t, size=11, color=t["text2"])
    s += box(
        lx + 14,
        86,
        194,
        122,
        "silver",
        [
            "40 typed tables, every row",
            "tagged with borough,",
            "neighborhood, precinct",
            "and taxi zone",
        ],
        t,
        stroke=t["accent"],
        title_color=t["accent"],
    )
    s += box(
        lx + 14,
        246,
        194,
        122,
        "gold",
        [
            "28 dbt models: dimensions,",
            "facts, an hourly trip",
            "aggregate, subway delays",
            "against the schedule",
        ],
        t,
        stroke=t["accent"],
        title_color=t["accent"],
    )
    s += [
        f'<line x1="{lx + 111}" y1="208" x2="{lx + 111}" y2="239" stroke="{t["accent"]}" stroke-width="1.5"/>',
        f'<path d="M{lx + 106.5},238 L{lx + 111},246 L{lx + 115.5},238 Z" fill="{t["accent"]}"/>',
    ]
    s += label(lx + 122, 231, "dbt, tested", t, size=11)
    s += label(lx + 14, 392, "time travel to any snapshot", t, size=11.5, color=t["text2"])
    s += label(lx + 14, 412, "daily compaction and expiry", t, size=11.5, color=t["text2"])
    s += label(lx + 14, 432, "over 800 million rows", t, size=11.5, color=t["text2"])
    s += arrow(cols["check"] + 196, 310, lx + 14, 150, t)

    # consumers, 180 wide
    s += box(
        cols["use"],
        246,
        180,
        88,
        "NYCynapse.Ai",
        ["questions in plain English,", "answered with SQL"],
        t,
    )
    s += box(cols["use"], 350, 180, 70, "Freshness", ["ops_source_freshness", "ops_feed_gap"], t)
    s += arrow(lx + 208, 290, cols["use"], 290, t)
    s += arrow(lx + 208, 340, cols["use"], 385, t)

    # ops strip
    s += [
        f'<rect x="24" y="476" width="{W - 48}" height="30" rx="8" fill="{t["card"]}" '
        f'stroke="{t["line"]}" stroke-width="1.2"/>'
    ]
    s += label(38, 496, "Ops tables in Postgres", t, size=12, color=t["text"], weight="600")
    s += label(
        196,
        496,
        "runs, checkpoints, upstream files, circuit breakers, quality results, "
        "quarantine, feed gaps, table coverage",
        t,
        size=12,
        color=t["text2"],
    )
    s.append("</svg>")
    return "\n".join(s)


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    for theme in THEMES:
        (OUT / f"architecture-{theme}.svg").write_text(draw(theme))
