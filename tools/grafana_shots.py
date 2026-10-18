#!/usr/bin/env python3
"""Screenshot Grafana panels for a time window, with headless Chrome (no Grafana image renderer needed).

    python3 tools/grafana_shots.py shots.json            # render every shot in a manifest
    python3 tools/grafana_shots.py shots.json --redo     # only shots whose PNG is missing or looks unrendered

A manifest is a JSON list of shots:

    [{"out": "experiments/img/x/loop-rejected.png",       # PNG to write
      "dashboard": "temporal-overview-v1",                 # dashboard uid
      "panel": 2405,                                       # panel id
      "from": 1792213000, "to": 1792213300,                # window, unix seconds
      "vars": {"namespace": "lab", "p": "0.99"},           # optional template variables
      "width": 1000, "height": 420}]                       # optional viewport

It loads Grafana's single-panel view (/d-solo/...) for exactly that window, so the lab's anonymous access must be
on (it is, in docker-compose.yml). If Pillow is installed, the blank area under the panel is trimmed.
"""
import json
import os
import subprocess
import sys
import tempfile
import urllib.parse

GRAFANA = os.environ.get("GRAFANA_URL", "http://localhost:3000")
CHROME = os.environ.get("CHROME", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")


def shot(s: dict) -> str:
    q = {"orgId": 1, "panelId": s["panel"], "from": int(s["from"] * 1000), "to": int(s["to"] * 1000),
         "theme": s.get("theme", "light"), "timezone": "utc"}
    for k, v in (s.get("vars") or {}).items():
        q[f"var-{k}"] = v
    url = f"{GRAFANA}/d-solo/{s['dashboard']}/x?{urllib.parse.urlencode(q)}"
    os.makedirs(os.path.dirname(s["out"]) or ".", exist_ok=True)
    for budget in (30000, 45000, 60000):   # a busy Grafana/Prometheus can need longer to draw the panel
        # A fresh profile per launch: concurrent headless Chromes sharing the default profile block on its lock.
        # A hard timeout: a hung Chrome is killed and retried instead of stalling the whole batch.
        with tempfile.TemporaryDirectory(prefix="grafana-shot-") as profile:
            try:
                subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars",
                                f"--user-data-dir={profile}", "--no-first-run", "--no-default-browser-check",
                                f"--window-size={s.get('width', 1000)},{s.get('height', 420)}",
                                f"--virtual-time-budget={budget}", f"--screenshot={s['out']}", url],
                               check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               timeout=budget / 1000 + 60)
            except subprocess.TimeoutExpired:
                print(f"  timeout rendering {s['out']}, retrying", file=sys.stderr)
                continue
        if os.path.exists(s["out"]) and not blank(s["out"]):
            break
    else:
        print(f"  WARNING: {s['out']} is still blank", file=sys.stderr)
    trim(s["out"])
    return s["out"]


def blank(path: str) -> bool:
    """True if Grafana had not drawn the panel yet: the image is one colour, or the chart area (above the legend)
    has almost no coloured pixels. Plotted series are coloured; axes, grid lines and text are grey."""
    try:
        from PIL import Image
    except ImportError:
        return os.path.getsize(path) < 5000
    img = Image.open(path).convert("RGB")
    lo, hi = img.convert("L").getextrema()
    if hi - lo < 10:
        return True
    w, h = img.size
    hsv = img.crop((40, 40, w - 10, int(h * 0.45))).convert("HSV")
    raw = hsv.tobytes()   # H, S, V bytes per pixel
    coloured = sum(1 for i in range(0, len(raw), 3) if raw[i + 1] > 80 and raw[i + 2] > 60)
    return coloured < 40


def trim(path: str) -> None:
    """Cut the uniform background below the panel (Grafana's page colour), keeping a small margin."""
    try:
        from PIL import Image
    except ImportError:
        return
    img = Image.open(path).convert("RGB")
    w, h = img.size
    bg = img.getpixel((w // 2, h - 1))
    bottom = h
    for y in range(h - 1, 0, -1):
        row = [img.getpixel((x, y)) for x in range(0, w, 7)]
        if any(sum(abs(a - b) for a, b in zip(p, bg)) > 12 for p in row):
            bottom = min(h, y + 6)
            break
    if bottom < h:
        img.crop((0, 0, w, bottom)).save(path)


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    redo = "--redo" in sys.argv
    if len(args) != 1:
        sys.exit(__doc__)
    with open(args[0]) as f:
        shots = json.load(f)
    for s in shots:
        if redo and os.path.exists(s["out"]) and not blank(s["out"]):
            continue
        print(shot(s))


if __name__ == "__main__":
    main()
