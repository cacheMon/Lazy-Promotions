import sys
from glob import glob
import re
import os
from urllib import parse
import pandas as pd
from pathlib import Path


general_pattern = re.compile(
    r"\.[A-Za-z0-9]+\s+"
    r"(?P<algo>[A-Za-z0-9_]+?)"
    r"(?:[-_](?P<param>[0-9.]+))?"
    r"\s+cache size\s+(?P<cache_size>[0-9A-Za-z]+),"
    r".*?miss ratio\s+(?P<miss_ratio>[0-9.]+),"
    r".*?throughput\s+(?P<throughput>[0-9.]+)"
    r".*?thread_num\s+(?P<num_thread>[0-9]+)"
)


def parse_size(size_str: str) -> int:
    size_str = size_str.strip().replace(" ", "")
    units = {
        "B": 1,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "TB": 1000**4,
        "PB": 1000**5,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
        "TiB": 1024**4,
        "PiB": 1024**5,
    }

    import re

    m = re.match(r"([0-9.]+)([A-Za-z]+)", size_str)
    if not m:
        raise ValueError(f"Invalid size string: {size_str}")

    num, unit = m.groups()
    multiplier = units.get(unit, 1)
    return int(float(num) * multiplier)


def parse_line(line: str):
    algo_name = {
        "FIFO": "FIFO",
        "Random": "Random",
        "lpFIFO_batch": "Batch",
        "LRU_Prob": "Prob",
        "LRU": "LRU",
        "LRU_delay": "Delay",
        "Clock": "FR",
    }
    match = general_pattern.search(line)
    if match:
        d = match.groupdict()
        entry = {
            "Algorithm": algo_name[d["algo"]],
            "Param": float(d["param"])
            if d["param"] is not None
            else 1.0
            if d["algo"] == "Clock"
            else 0.0,
            "Miss Ratio": float(d["miss_ratio"]),
            "Cache Size": parse_size(d["cache_size"]),
            "Throughput": float(d["throughput"]),
            "Thread": float(d["num_thread"]),
        }
        return entry
    return None


def ReadData(path: str):
    rows = []
    outputs = Path(path).rglob("*")
    for file in outputs:
        if os.path.isdir(file) or file.suffix.lower() != ".txt":
            continue
        with open(file, "r") as f:
            for _, line in enumerate(f):
                row = parse_line(line)
                if row:
                    rows.append(row)
    return pd.DataFrame(rows)


def main():
    if len(sys.argv) < 2:
        raise ValueError("Missing required argument: data path")

    data = ReadData(sys.argv[1])
    script_dir = Path(__file__).resolve().parent
    os.makedirs(script_dir / "data", exist_ok=True)
    data.to_feather(script_dir / "data/scalability.feather")
    data.to_csv(script_dir / "data/scalability.csv")


if __name__ == "__main__":
    main()

# keys = ["Trace Path", "Cache Size", "Algorithm", "Config"]
# data = data.drop_duplicates(subset=keys)
# script_dir = Path(__file__).resolve().parent
#
# os.makedirs(script_dir / "data", exist_ok=True)
# data.to_feather(script_dir / "data/data.feather")
# data.to_csv(script_dir / "data/data.csv")
