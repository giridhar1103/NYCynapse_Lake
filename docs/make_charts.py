"""Draw the README charts from the lake. Each chart is written in a light and a dark version.

.venv/bin/pip install matplotlib
.venv/bin/python docs/make_charts.py
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.collections import PolyCollection  # noqa: E402
from matplotlib.dates import DateFormatter, MonthLocator  # noqa: E402

from nycynapse_lake import lake  # noqa: E402
from nycynapse_lake.config import Settings  # noqa: E402

OUT = Path(__file__).parent / "img"

THEMES = {
    "light": {
        "surface": "#fcfcfb",
        "text": "#0b0b0b",
        "text2": "#52514e",
        "muted": "#898781",
        "grid": "#e1e0d9",
        "axis": "#c3c2b7",
        "empty": "#f0efec",
        "series": ["#2a78d6", "#eb6834", "#1baf7a"],
        "ramp": ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"],
    },
    "dark": {
        "surface": "#1a1a19",
        "text": "#ffffff",
        "text2": "#c3c2b7",
        "muted": "#898781",
        "grid": "#2c2c2a",
        "axis": "#383835",
        "empty": "#2c2c2a",
        "series": ["#3987e5", "#d95926", "#199e70"],
        "ramp": ["#0d366b", "#104281", "#184f95", "#256abf", "#3987e5", "#6da7ec", "#9ec5f4"],
    },
}


def frame(theme, width=9.6, height=4.6):
    t = THEMES[theme]
    fig, ax = plt.subplots(figsize=(width, height), dpi=160)
    fig.patch.set_facecolor(t["surface"])
    ax.set_facecolor(t["surface"])
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(t["axis"])
    ax.tick_params(colors=t["muted"], labelsize=9, length=0, pad=6)
    ax.grid(axis="y", color=t["grid"], linewidth=0.8)
    ax.set_axisbelow(True)
    return fig, ax, t


def titles(fig, t, title, subtitle, source):
    fig.text(0.06, 0.95, title, color=t["text"], fontsize=13, fontweight="bold", va="top")
    fig.text(0.06, 0.885, subtitle, color=t["text2"], fontsize=10, va="top")
    fig.text(0.06, 0.03, source, color=t["muted"], fontsize=8, va="bottom")


def save(fig, name, theme):
    fig.savefig(OUT / f"{name}-{theme}.png", facecolor=fig.get_facecolor())
    plt.close(fig)


def weekly_trips(con):
    rows = con.execute("""
        SELECT date_trunc('week', pickup_date) AS week, service, sum(trips) / 7.0 AS per_day
        FROM lake.gold.agg_trips_zone_hourly
        WHERE pickup_date >= DATE '2024-01-01' AND service IN ('uber', 'lyft', 'yellow')
          AND date_trunc('week', pickup_date) < (SELECT date_trunc('week', max(pickup_date))
                                                 FROM lake.gold.agg_trips_zone_hourly)
        GROUP BY ALL ORDER BY week
    """).fetchall()
    series = {}
    for week, service, per_day in rows:
        series.setdefault(service, ([], []))
        series[service][0].append(week)
        series[service][1].append(per_day / 1000)
    names = {"uber": "Uber", "lyft": "Lyft", "yellow": "Yellow taxi"}
    for theme in THEMES:
        fig, ax, t = frame(theme)
        fig.subplots_adjust(left=0.06, right=0.86, top=0.78, bottom=0.14)
        for color, key in zip(t["series"], ("uber", "lyft", "yellow"), strict=True):
            x, y = series[key]
            ax.plot(x, y, color=color, linewidth=2, solid_capstyle="round")
            ax.annotate(
                f"{names[key]}  {y[-1]:,.0f}k",
                (x[-1], y[-1]),
                xytext=(8, 0),
                textcoords="offset points",
                va="center",
                fontsize=9.5,
                color=t["text"],
            )
        ax.set_ylim(0, None)
        ax.yaxis.set_major_formatter(lambda v, _: f"{v:,.0f}k")
        ax.xaxis.set_major_locator(MonthLocator(bymonth=(1, 4, 7, 10)))
        ax.xaxis.set_major_formatter(DateFormatter("%b %Y"))
        import datetime as dt

        start = dt.date(2025, 1, 5)
        ax.axvline(start, color=t["muted"], linewidth=1, linestyle=(0, (3, 3)))
        ax.text(
            start,
            ax.get_ylim()[1] * 0.97,
            "  Congestion pricing starts",
            color=t["text2"],
            fontsize=9,
            va="top",
        )
        titles(
            fig,
            t,
            "Trips per day, weekly average",
            "App rides and yellow taxis, January 2024 to August 2026",
            "Source: NYC TLC trip records, 768 million trips, via lake.gold.agg_trips_zone_hourly",
        )
        save(fig, "trips-per-day", theme)


def subway_delays(con):
    rows = con.execute("""
        SELECT greatest(least(round(delay_seconds / 30.0) * 0.5, 15), -5) AS minutes, count(*)
        FROM lake.gold.fct_subway_arrival
        WHERE delay_seconds IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    stats = con.execute("""
        SELECT count(*), median(delay_seconds) / 60.0,
               avg(CASE WHEN delay_seconds <= 300 THEN 1.0 ELSE 0.0 END),
               min(arrived_date), max(arrived_date)
        FROM lake.gold.fct_subway_arrival WHERE delay_seconds IS NOT NULL
    """).fetchone()
    n, median, on_time, first, last = stats
    total = sum(c for _, c in rows)
    for theme in THEMES:
        fig, ax, t = frame(theme, height=4.4)
        fig.subplots_adjust(left=0.06, right=0.97, top=0.76, bottom=0.16)
        xs = [float(m) for m, _ in rows]
        ys = [c / total * 100 for _, c in rows]
        colors = [t["series"][0] if x <= 5 else t["ramp"][2 if theme == "light" else 4] for x in xs]
        ax.bar(xs, ys, width=0.42, color=colors, edgecolor=t["surface"], linewidth=0)
        ax.axvline(5.25, color=t["muted"], linewidth=1, linestyle=(0, (3, 3)))
        ax.text(
            5.45,
            max(ys) * 0.95,
            "More than 5 minutes late\ncounts as late",
            color=t["text2"],
            fontsize=9,
            va="top",
        )
        ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0f}%")
        ax.set_xlabel("Minutes behind schedule (negative is early)", color=t["text2"], fontsize=9)
        ax.set_xticks(range(-5, 16, 5))
        ax.set_xticklabels(
            [
                "-5 or more early" if v == -5 else ("15+" if v == 15 else str(v))
                for v in range(-5, 16, 5)
            ]
        )
        span = (
            first.strftime("%-d %b %Y")
            if first == last
            else (f"{first.strftime('%-d %b')} to {last.strftime('%-d %b %Y')}")
        )
        titles(
            fig,
            t,
            f"{on_time:.0%} of subway arrivals were on time",
            f"{n:,} observed arrivals matched to the schedule, {span}. Median {median:+.1f} min.",
            "Source: MTA GTFS-realtime, tracked every 30 seconds, via lake.gold.fct_subway_arrival",
        )
        save(fig, "subway-delays", theme)


def _nice(value: float) -> float:
    """Round a class break to two significant figures so the legend reads cleanly."""
    from math import floor, log10

    step = 10 ** (floor(log10(value)) - 1)
    return round(value / step) * step


def noise_map(con):
    rows = con.execute("""
        WITH counts AS (
            SELECT nta_code, count(*) AS n FROM lake.gold.fct_service_request
            WHERE complaint_type ILIKE 'noise%' GROUP BY 1
        )
        SELECT n.nta_code, n.nta_kind = 'residential' AS residential,
               coalesce(c.n, 0) / (n.area_sq_ft / 27878400.0) AS per_sq_mi,
               ST_AsGeoJSON(ST_SimplifyPreserveTopology(n.geom, 0.0002)) AS shape
        FROM lake.silver.geo_nta n LEFT JOIN counts c USING (nta_code)
    """).fetchall()
    rates = sorted(r for _, res, r, _ in rows if res and r > 0)
    cuts = [_nice(rates[int(len(rates) * q / 7)]) for q in range(1, 7)]

    def polys(shape):
        g = json.loads(shape)
        parts = g["coordinates"] if g["type"] == "MultiPolygon" else [g["coordinates"]]
        return [p[0] for p in parts]

    for theme in THEMES:
        t = THEMES[theme]
        fig, ax = plt.subplots(figsize=(8.2, 8.6), dpi=160)
        fig.patch.set_facecolor(t["surface"])
        ax.set_facecolor(t["surface"])
        ax.set_axis_off()
        fig.subplots_adjust(left=0.02, right=0.98, top=0.86, bottom=0.08)
        shapes, colors = [], []
        for _, residential, rate, shape in rows:
            bucket = sum(rate > c for c in cuts)
            color = t["ramp"][bucket] if residential else t["empty"]
            for ring in polys(shape):
                shapes.append(ring)
                colors.append(color)
        ax.add_collection(
            PolyCollection(shapes, facecolors=colors, edgecolors=t["surface"], linewidths=0.6)
        )
        ax.autoscale_view()
        ax.set_aspect(1 / 0.758)  # longitude is shorter than latitude at New York's latitude
        fig.text(
            0.04,
            0.955,
            "311 noise complaints per square mile",
            color=t["text"],
            fontsize=13,
            fontweight="bold",
            va="top",
        )
        fig.text(
            0.04,
            0.925,
            "By neighborhood (2020 NTA), January 2024 to now. Parks, airports "
            "and cemeteries in gray.",
            color=t["text2"],
            fontsize=10,
            va="top",
        )
        labels = (
            [f"under {cuts[0]:,.0f}"]
            + [f"{a:,.0f} to {b:,.0f}" for a, b in zip(cuts, cuts[1:], strict=False)]
            + [f"{cuts[-1]:,.0f} and up"]
        )
        for i, (label, color) in enumerate(zip(labels, t["ramp"], strict=True)):
            y = 0.86 - i * 0.032
            fig.patches.append(
                plt.Rectangle(
                    (0.06, y),
                    0.025,
                    0.022,
                    transform=fig.transFigure,
                    facecolor=color,
                    edgecolor="none",
                )
            )
            fig.text(0.095, y + 0.011, label, color=t["text2"], fontsize=9, va="center")
        fig.text(
            0.04,
            0.03,
            "Source: NYC 311 service requests, 2.2 million noise complaints, "
            "tagged to neighborhoods at load time",
            color=t["muted"],
            fontsize=8,
        )
        save(fig, "noise-map", theme)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = lake.connect(Settings.from_env(), read_only=True)
    weekly_trips(con)
    subway_delays(con)
    noise_map(con)


if __name__ == "__main__":
    main()
