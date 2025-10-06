import sys
import pandas as pd
from pathlib import Path


def ProcessData(df: pd.DataFrame):
    df = df.round(4)
    df = df.query("`Ignore Obj Size` == 1")
    df = df.sort_values(by="Trace Path")
    df = df.query("`Real Cache Size` >= 10")
    df = df.query("`Request` >= 1_000_000")

    df["Trace Group"] = df["Trace Path"].apply(lambda x: Path(x).parts[0])
    df["Miss"] = df["Request"] * df["Miss Ratio"]

    fifo_miss_ratio = df.set_index(["Cache Size", "Trace Path"]).index.map(
        df.set_index("Algorithm")
        .loc["FIFO"]
        .set_index(["Cache Size", "Trace Path"])["Miss Ratio"]
    )
    df["Relative Miss Ratio [FIFO]"] = df["Miss Ratio"] / fifo_miss_ratio
    df["Promotion Efficiency"] = (
        (fifo_miss_ratio - df["Miss Ratio"]) * df["Request"] / df["Reinserted"]
    )

    lru_promos = df.set_index(["Cache Size", "Trace Path"]).index.map(
        df.set_index("Algorithm")
        .loc["LRU"]
        .set_index(["Cache Size", "Trace Path"])["Reinserted"]
    )
    df["Relative Promotion [LRU]"] = df["Reinserted"] / lru_promos

    lru_miss = df.set_index(["Cache Size", "Trace Path"]).index.map(
        df.set_index("Algorithm")
        .loc["LRU"]
        .set_index(["Cache Size", "Trace Path"])["Miss Ratio"]
    )
    df["Relative Miss Ratio [LRU]"] = df["Miss Ratio"] / lru_miss

    base_clock_condition = (df["Algorithm"] == "FR") & (df["Bit"] == 1)

    base_clock_promos = df.set_index(["Cache Size", "Trace Path"]).index.map(
        df.loc[base_clock_condition].set_index(["Cache Size", "Trace Path"])[
            "Reinserted"
        ]
    )
    df["Relative Promotion [Base FR]"] = df["Reinserted"] / base_clock_promos

    base_clock_miss = df.set_index(["Cache Size", "Trace Path"]).index.map(
        df.loc[base_clock_condition].set_index(["Cache Size", "Trace Path"])[
            "Miss Ratio"
        ]
    )
    df["Relative Miss Ratio [Base FR]"] = df["Miss Ratio"] / base_clock_miss

    bit_clock_promos = df.set_index(["Cache Size", "Trace Path", "Bit"]).index.map(
        df.loc[df["Algorithm"] == "FR"].set_index(["Cache Size", "Trace Path", "Bit"])[
            "Reinserted"
        ]
    )
    df["Relative Promotion [Bit FR]"] = df["Reinserted"] / bit_clock_promos

    bit_clock_miss = df.set_index(["Cache Size", "Trace Path", "Bit"]).index.map(
        df.loc[df["Algorithm"] == "FR"].set_index(["Cache Size", "Trace Path", "Bit"])[
            "Miss Ratio"
        ]
    )
    df["Relative Miss Ratio [Bit FR]"] = df["Miss Ratio"] / bit_clock_miss

    adv_promos = df.set_index(["Cache Size", "Trace Path", "Algorithm"]).index.map(
        df.set_index("Variant")
        .loc["LRU"]
        .set_index(["Cache Size", "Trace Path", "Algorithm"])["Reinserted"]
    )
    df["Relative Promotion [Adv]"] = df["Reinserted"] / adv_promos

    adv_miss = df.set_index(["Cache Size", "Trace Path", "Algorithm"]).index.map(
        df.set_index("Variant")
        .loc["LRU"]
        .set_index(["Cache Size", "Trace Path", "Algorithm"])["Miss Ratio"]
    )
    df["Relative Miss Ratio [Adv]"] = df["Miss Ratio"] / adv_miss
    return df


script_dir = Path(__file__).resolve().parent
df = pd.read_feather(script_dir / "data/data.feather")

processed = ProcessData(
    df.query("~Trace.str.contains('zipf', case=False)", engine="python")
)
processed.to_csv(script_dir / "data/processed.csv")
processed.to_feather(script_dir / "data/processed.feather")

df_throughput = df.query("Trace.str.contains('zipf', case=False)", engine="python")
if not df_throughput.empty:
    df_throughput = df.loc[:, ["Algorithm", "Scale", "Bit", "Throughput"]]
    df_throughput = (
        df.groupby(["Algorithm", "Scale", "Bit"]).mean(numeric_only=True).reset_index()
    )
    df_mean = (
        df.groupby(["Algorithm", "Scale", "Bit"]).mean(numeric_only=True).reset_index()
    )
    overall_throughput = pd.merge(
        df_throughput,
        df_mean,
        left_on=["Algorithm", "Scale", "Bit"],
        right_on=["Algorithm", "Scale", "Bit"],
        how="inner",
    )
    overall_throughput.to_csv(script_dir / "data/throughput.csv")
    overall_throughput.to_feather(script_dir / "data/throughput.feather")
else:
    print("Throughput data is not available")
