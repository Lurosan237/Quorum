"""netwatch: a week of your real internet and power, recorded for Quorum's simulator.

Leave it running on your laptop (plugged in, sleep turned off) for about a week:

    python netwatch.py

Every 5 seconds it times a connection to three public servers and notes whether the laptop is
on mains power, appending one line per check to netwatch-data/YYYY-MM-DD.csv. Nothing else is
recorded: no websites, no traffic, no location. Stop it any time with Ctrl+C; run it again and it
carries on in the same files.

What the simulator learns from it:
- how often messages are lost and how slow they are, hour by hour;
- when the internet drops out completely, and for how long;
- when the power goes (the laptop switches to battery) and when it comes back; and if the battery
  runs out, the gap in the log shows how long the machine was off.

Needs only Python 3.8+. Battery status is read directly on Windows, Linux and macOS.
"""

from __future__ import annotations

import csv
import datetime as dt
import os
import platform
import socket
import subprocess
import sys
import time
from pathlib import Path

INTERVAL_S = 5
TIMEOUT_S = 2.0
# Public servers that answer anywhere: two by address (no DNS needed), one by name (DNS too)
TARGETS = [("1.1.1.1", 443), ("8.8.8.8", 53), ("www.google.com", 443)]
OUT = Path(__file__).resolve().parent / "netwatch-data"


def connect_ms(host: str, port: int) -> float | None:
    """Time to open a TCP connection (one network round trip, plus DNS for a name), or None."""
    start = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=TIMEOUT_S):
            return (time.perf_counter() - start) * 1000
    except OSError:
        return None


def power() -> tuple[str, str]:
    """('mains' | 'battery' | 'unknown', battery percent or '')."""
    system = platform.system()
    try:
        if system == "Windows":
            import ctypes

            class Status(ctypes.Structure):
                _fields_ = [("ACLineStatus", ctypes.c_byte), ("BatteryFlag", ctypes.c_byte),
                            ("BatteryLifePercent", ctypes.c_byte), ("SystemStatusFlag", ctypes.c_byte),
                            ("BatteryLifeTime", ctypes.c_ulong), ("BatteryFullLifeTime", ctypes.c_ulong)]

            s = Status()
            if ctypes.windll.kernel32.GetSystemPowerStatus(ctypes.byref(s)):
                line = {1: "mains", 0: "battery"}.get(s.ACLineStatus, "unknown")
                pct = "" if s.BatteryLifePercent == -1 or s.BatteryLifePercent == 255 else str(s.BatteryLifePercent)
                return line, pct
        elif system == "Linux":
            base = Path("/sys/class/power_supply")
            online = [p for p in base.glob("*") if (p / "online").exists() and (p / "type").read_text().strip() == "Mains"]
            bats = [p for p in base.glob("BAT*")]
            line = "unknown"
            if online:
                line = "mains" if (online[0] / "online").read_text().strip() == "1" else "battery"
            pct = (bats[0] / "capacity").read_text().strip() if bats and (bats[0] / "capacity").exists() else ""
            return line, pct
        elif system == "Darwin":
            out = subprocess.run(["pmset", "-g", "batt"], capture_output=True, text=True, timeout=3).stdout
            line = "mains" if "AC Power" in out else "battery" if "Battery Power" in out else "unknown"
            pct = ""
            if "%" in out:
                pct = out.split("%")[0].split()[-1]
            return line, pct
    except Exception:  # noqa: BLE001 - power status is a bonus, never a reason to stop
        pass
    return "unknown", ""


def main() -> None:
    OUT.mkdir(exist_ok=True)
    print(f"netwatch: checking every {INTERVAL_S} s, writing to {OUT}. Ctrl+C to stop.")
    last_power = None
    lost_streak = 0
    try:
        while True:
            tick = time.time()
            now = dt.datetime.now(dt.timezone.utc)
            line, pct = power()
            results = [(f"{h}:{p}", connect_ms(h, p)) for h, p in TARGETS]
            path = OUT / f"{now:%Y-%m-%d}.csv"
            new = not path.exists()
            with open(path, "a", newline="") as f:
                w = csv.writer(f)
                if new:
                    w.writerow(["time_utc", "target", "ms", "power", "battery"])
                for target, ms in results:
                    w.writerow([now.isoformat(timespec="seconds"), target, "" if ms is None else f"{ms:.1f}", line, pct])
            ok = [ms for _, ms in results if ms is not None]
            lost_streak = 0 if ok else lost_streak + 1
            if line != last_power and last_power is not None:
                print(f"\n{now:%H:%M:%S}  power: {last_power} -> {line}")
            last_power = line
            status = f"{min(ok):.0f} ms" if ok else f"no internet ({lost_streak * INTERVAL_S} s)"
            sys.stdout.write(f"\r{dt.datetime.now():%a %H:%M:%S}  {len(ok)}/3 reached, best {status:<22} power: {line} {pct}%   ")
            sys.stdout.flush()
            time.sleep(max(0.0, INTERVAL_S - (time.time() - tick)))
    except KeyboardInterrupt:
        print("\nstopped. The data is in", OUT)


if __name__ == "__main__":
    if os.environ.get("NETWATCH_ONCE"):
        print(power(), [(f"{h}:{p}", connect_ms(h, p)) for h, p in TARGETS])
    else:
        main()
