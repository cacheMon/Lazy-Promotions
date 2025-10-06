import pandas as pd
import matplotlib_wrapper as plt_wrapper
import style
import numpy as np


def figure1a(df: pd.DataFrame):
    data = df.query(
        '(Algorithm == "Delay" and Scale == 0.2) or'
        '(Algorithm == "Prob" and Scale == 0.5) or'
        '(Algorithm == "Batch" and Scale == 0.5) or'
        '(Algorithm == "FR" and Bit == 1) or'
    )
    plt_wrapper.plt_scatter(
        data,
        "Relative Miss Ratio [LRU]",
        "Relative Throughput [LRU]",
        x_label="Miss ratio relative to LRU",
        y_label="Throughput relative to LRU",
        hue="algorithm",
        markers=style.markers,
        palette=style.palette,
        output_pdf="figure1a.pdf",
    )
    pass


def figure1b(df: pd.DataFrame):
    data = df.query(
        '(Algorithm == "Delay" and Scale == 0.2) or'
        '(Algorithm == "Prob" and Scale == 0.5) or'
        '(Algorithm == "Batch" and Scale == 0.5) or'
        '(Algorithm == "FR" and Bit == 1) or'
        '(Algorithm == "D-FR" and Scale == 0.05 and Bit == 1) or'
        '(Algorithm == "AGE" and Scale == 0.5)'
    )
    mean = data.groupby(["Algorithm"]).mean(numeric_only=True).reset_index()
    plt_wrapper.plt_scatter(
        mean,
        y="Relative Promotion [LRU]",
        y_label="Promotions relative to LRU",
        x="Relative Miss Ratio [LRU]",
        x_label="Miss ratio relative to LRU",
        hue="Algorithm",
        markers=style.markers,
        palette={
            "Batch": "#eff3ff",
            "Prob": "#bdd7e7",
            "Delay": "#6baed6",
            "FR": "#3182bd",
            "D-FR": "#31a354",
            "AGE": "#bae4b3",
        },
        order=[
            "Delay",
            "FR",
            "Batch",
            "Prob",
            "D-FR",
            "AGE",
        ],
        output_pdf="figures/figure1b.pdf",
    )


def figure2a(df: pd.DataFrame):
    data = df.query('Algorithm == "Prob"')
    plt_wrapper.plt_box(
        data,
        y="Relative Promotion [LRU]",
        y_label="Promotions relative to LRU",
        x="Scale",
        x_label="Prob",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure2a.pdf",
        marker_size=11,
        invert=True,
        tick_step=0.2,
    )


def figure2b(df: pd.DataFrame):
    data = df.query('Algorithm == "Prob"')
    plt_wrapper.plt_box(
        data,
        y="Relative Throughput [LRU]",
        y_label="Throughput relative to LRU",
        x="Scale",
        x_label="Prob",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure2b.pdf",
        marker_size=11,
        invert=True,
    )


def figure2c(df: pd.DataFrame):
    data = df.query('Algorithm == "Prob"')
    plt_wrapper.plt_box(
        data,
        y="Relative Miss Ratio [LRU]",
        y_label="Miss ratio relative to LRU",
        x="Scale",
        x_label="Prob",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure2c.pdf",
        marker_size=11,
        invert=True,
    )


def figure3a(df: pd.DataFrame):
    data = df.query('Algorithm == "Batch"')
    plt_wrapper.plt_box(
        data,
        y="Relative Promotion [LRU]",
        y_label="Promotions relative to LRU",
        x="Scale",
        x_label="Batch size",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure3a.pdf",
        marker_size=11,
        tick_step=0.2,
    )


def figure3b(df: pd.DataFrame):
    data = df.query('Algorithm == "Batch"')
    plt_wrapper.plt_box(
        data,
        y="Relative Throughput [LRU]",
        y_label="Throughput relative to LRU",
        x="Scale",
        x_label="Batch size",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure3b.pdf",
        marker_size=11,
    )


def figure3c(df: pd.DataFrame):
    data = df.query('Algorithm == "Batch"')
    plt_wrapper.plt_box(
        data,
        y="Relative Miss Ratio [LRU]",
        y_label="Miss ratio relative to LRU",
        x="Scale",
        x_label="Batch size",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure3c.pdf",
        marker_size=11,
    )


def figure4a(df: pd.DataFrame):
    data = df.query('Algorithm == "Delay"')
    plt_wrapper.plt_box(
        data,
        y="Relative Promotion [LRU]",
        y_label="Promotions relative to LRU",
        x="Scale",
        tick_step=0.2,
        x_label="Delay ratio",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure4a.pdf",
        marker_size=11,
    )


def figure4b(df: pd.DataFrame):
    data = df.query('Algorithm == "Delay"')
    plt_wrapper.plt_box(
        data,
        y="Relative Throughput [LRU]",
        y_label="Throughput relative to LRU",
        x="Scale",
        x_label="Delay",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure4b.pdf",
        marker_size=11,
    )


def figure4c(df: pd.DataFrame):
    data = df.query('Algorithm == "Delay"')
    plt_wrapper.plt_box(
        data,
        y="Relative Miss Ratio [LRU]",
        y_label="Miss ratio relative to LRU",
        x="Scale",
        x_label="Delay ratio",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure4c.pdf",
        marker_size=11,
    )


def figure5a(df: pd.DataFrame):
    data = df.query('Algorithm == "FR"')
    plt_wrapper.plt_box(
        data,
        y="Relative Promotion [LRU]",
        y_label="Promotions relative to LRU",
        x="Bit",
        x_label="# Frequency bits",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure5a.pdf",
        marker_size=11,
        tick_step=0.2,
    )


def figure5b(df: pd.DataFrame):
    data = df.query('Algorithm == "FR"')
    plt_wrapper.plt_box(
        data,
        y="Relative Throughput [LRU]",
        y_label="Throughput relative to LRU",
        x="Bit",
        x_label="# Frequency bits",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure5b.pdf",
        marker_size=11,
    )


def figure5c(df: pd.DataFrame):
    data = df.query('Algorithm == "FR"')
    plt_wrapper.plt_box(
        data,
        y="Relative Miss Ratio [LRU]",
        y_label="Miss ratio relative to LRU",
        x="Bit",
        x_label="# Frequency bits",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure5c.pdf",
        marker_size=11,
    )


def figure6a(df: pd.DataFrame):
    data = df.query('Algorithm == "Random"')
    plt_wrapper.plt_box(
        data,
        y="Relative Throughput [LRU]",
        y_label="Throughput relative to LRU",
        x="Scale",
        x_label="Sample size",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure6a.pdf",
        marker_size=11,
    )


def figure6b(df: pd.DataFrame):
    data = df.query('Algorithm == "Random"')
    plt_wrapper.plt_box(
        data,
        y="Relative Miss Ratio [LRU]",
        y_label="Miss ratio relative to LRU",
        x="Scale",
        x_label="Sample size",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure6b.pdf",
        marker_size=11,
    )


def figure7a(df: pd.DataFrame):
    data = df.query('Algorithm == "Prob"')
    plt_wrapper.plt_box(
        data,
        y="Promotion Efficiency",
        y_label="Promotion efficiency",
        x="Scale",
        x_label="Prob",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure7a.pdf",
        marker_size=11,
        invert=True,
        tick_step=0.2,
    )


def figure7b(df: pd.DataFrame):
    data = df.query('Algorithm == "Batch"')
    plt_wrapper.plt_box(
        data,
        y="Promotion Efficiency",
        y_label="Promotion efficiency",
        x="Scale",
        x_label="Prob",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure7b.pdf",
        marker_size=11,
        tick_step=0.2,
    )


def figure7c(df: pd.DataFrame):
    data = df.query('Algorithm == "Delay"')
    plt_wrapper.plt_box(
        data,
        y="Promotion Efficiency",
        y_label="Promotion efficiency",
        x="Scale",
        x_label="Delay",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure7c.pdf",
        marker_size=11,
        tick_step=0.2,
    )


def figure7d(df: pd.DataFrame):
    data = df.query('Algorithm == "FR"')
    plt_wrapper.plt_box(
        data,
        y="Promotion Efficiency",
        y_label="Promotion efficiency",
        x="Bit",
        x_label="# Frequency bits",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure7d.pdf",
        marker_size=11,
        tick_step=0.2,
    )


def figure8a(df: pd.DataFrame):
    data = df.query('Algorithm == "ARC"')
    plt_wrapper.plt_box(
        data,
        y="Relative Promotion [Adv]",
        y_label="Promotions relative to ARC",
        x="Variant",
        x_label="",
        x_size=12,
        hue="Algorithm",
        palette=["#EE964B"],
        output_pdf="figures/figure8a.pdf",
        marker_size=11,
        tick_step=0.2,
        order=["Prob", "Batch", "Delay", "FR"],
    )


def figure8b(df: pd.DataFrame):
    data = df.query('Algorithm == "ARC"')
    plt_wrapper.plt_box(
        data,
        y="Relative Miss Ratio [Adv]",
        y_label="Miss ratio relative to ARC",
        x="Variant",
        x_label="",
        x_size=12,
        hue="Algorithm",
        palette=["#EE964B"],
        output_pdf="figures/figure8b.pdf",
        marker_size=11,
        tick_step=0.2,
        order=["Prob", "Batch", "Delay", "FR"],
    )


def figure8c(df: pd.DataFrame):
    data = df.query('Algorithm == "TwoQ"')
    plt_wrapper.plt_box(
        data,
        y="Relative Promotion [Adv]",
        y_label="Promotions relative to 2Q",
        x="Variant",
        x_label="",
        x_size=12,
        hue="Algorithm",
        palette=["#EE964B"],
        output_pdf="figures/figure8c.pdf",
        marker_size=11,
        tick_step=0.2,
        order=["Prob", "Batch", "Delay", "FR"],
    )


def figure8d(df: pd.DataFrame):
    data = df.query('Algorithm == "TwoQ"')
    plt_wrapper.plt_box(
        data,
        y="Relative Miss Ratio [Adv]",
        y_label="Miss ratio relative to 2Q",
        x="Variant",
        x_label="",
        x_size=12,
        hue="Algorithm",
        palette=["#EE964B"],
        output_pdf="figures/figure8d.pdf",
        marker_size=11,
        tick_step=0.2,
        order=["Prob", "Batch", "Delay", "FR"],
    )


def figure9a(df: pd.DataFrame):
    data = df.query('Algorithm == "Belady-RandomLRU"')
    data.loc[data["Scale"] > 10, "Scale"] = np.inf

    def format_scale(v):
        if np.isinf(v):
            return "inf"
        return str(int(v))

    data["Scale"] = data["Scale"].apply(format_scale)
    plt_wrapper.plt_box(
        data,
        y="Relative Miss Ratio [LRU]",
        y_label="Miss ratio relative to LRU",
        x="Scale",
        x_label="Factor",
        x_size=12,
        tick_step=0.05,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure9a.pdf",
        marker_size=11,
    )


def figure9b(df: pd.DataFrame):
    data = df.query('Algorithm == "Belady-Random"')
    data.loc[data["Scale"] > 10, "Scale"] = np.inf

    def format_scale(v):
        if np.isinf(v):
            return "inf"
        return str(int(v))

    data["Scale"] = data["Scale"].apply(format_scale)
    plt_wrapper.plt_box(
        data,
        y="Relative Miss Ratio [LRU]",
        y_label="Miss ratio relative to LRU",
        x="Scale",
        x_label="Factor",
        x_size=12,
        tick_step=0.05,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure9b.pdf",
        marker_size=11,
    )


def figure9c(df: pd.DataFrame):
    data = df.query('Algorithm == "Belady-Random"')
    data.loc[data["Scale"] > 10, "Scale"] = np.inf

    def format_scale(v):
        if np.isinf(v):
            return "inf"
        return str(int(v))

    data["Scale"] = data["Scale"].apply(format_scale)
    plt_wrapper.plt_box(
        data,
        y="BEE Fraction",
        y_label="Fraction of Belady early eviction",
        x="Scale",
        x_label="Factor",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure9c.pdf",
        marker_size=11,
    )


def figure10a(df: pd.DataFrame):
    data = df.query('Algorithm == "Offline-FR"')
    plt_wrapper.plt_box(
        data,
        y="Relative Promotion [LRU]",
        y_label="Promotions relative to LRU",
        x="Scale",
        x_label="# Iteration",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure10a.pdf",
        marker_size=11,
        tick_step=0.2,
    )


def figure10b(df: pd.DataFrame):
    data = df.query('Algorithm == "Offline-FR"')
    plt_wrapper.plt_box(
        data,
        y="Relative Miss Ratio [LRU]",
        y_label="Miss ratio relative to LRU",
        x="Scale",
        x_label="# Iteration",
        x_size=12,
        tick_step=0.02,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure10b.pdf",
        marker_size=11,
    )


def figure10c(df: pd.DataFrame):
    data = df.query('Algorithm == "Offline-FR"')
    plt_wrapper.plt_box(
        data,
        y="Promotion Efficiency",
        y_label="Promotion efficiency",
        x="Scale",
        x_label="# Iteration",
        x_size=12,
        tick_step=0.2,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure10c.pdf",
        marker_size=11,
    )


def figure11a(df: pd.DataFrame):
    data = df.query(
        '(Algorithm == "Delay" and Scale == 0.2) or'
        '(Algorithm == "Prob" and Scale == 0.4) or'
        '(Algorithm == "Batch" and Scale == 0.5) or'
        '(Algorithm == "FR" and Bit == 1) or'
        '(Algorithm == "D-FR" and Scale == 0.05 and Bit == 1) or'
        '(Algorithm == "AGE" and Scale == 0.5)'
    )
    plt_wrapper.plt_box(
        data,
        y="Relative Promotion [LRU]",
        y_label="Promotions relative to LRU",
        x="Algorithm",
        x_label="",
        x_size=12,
        hue="Algorithm",
        output_pdf="figures/figure11a.pdf",
        marker_size=11,
        tick_step=0.2,
        dodge=False,
        palette=style.palette,
        order=["Prob", "Batch", "Delay", "FR", "D-FR", "AGE"],
    )


def figure11b(df: pd.DataFrame):
    data = df.query(
        '(Algorithm == "Delay" and Scale == 0.2) or'
        '(Algorithm == "Prob" and Scale == 0.4) or'
        '(Algorithm == "Batch" and Scale == 0.5) or'
        '(Algorithm == "FR" and Bit == 1) or'
        '(Algorithm == "D-FR" and Scale == 0.05 and Bit == 1) or'
        '(Algorithm == "AGE" and Scale == 0.5)'
    )
    plt_wrapper.plt_box(
        data,
        y="Relative Miss Ratio [LRU]",
        y_label="Miss ratio relative to LRU",
        x="Algorithm",
        x_label="",
        x_size=12,
        hue="Algorithm",
        output_pdf="figures/figure11b.pdf",
        marker_size=11,
        tick_step=0.2,
        dodge=False,
        palette=style.palette,
        order=["Prob", "Batch", "Delay", "FR", "D-FR", "AGE"],
    )


def figure11c(df: pd.DataFrame):
    data = df.query(
        '(Algorithm == "Delay" and Scale == 0.2) or'
        '(Algorithm == "Prob" and Scale == 0.4) or'
        '(Algorithm == "Batch" and Scale == 0.5) or'
        '(Algorithm == "FR" and Bit == 1) or'
        '(Algorithm == "D-FR" and Scale == 0.05 and Bit == 1) or'
        '(Algorithm == "AGE" and Scale == 0.5)'
    )
    plt_wrapper.plt_box(
        data,
        y="Promotion Efficiency",
        y_label="Promotion efficiency",
        x="Algorithm",
        x_label="",
        x_size=12,
        hue="Algorithm",
        output_pdf="figures/figure11c.pdf",
        marker_size=11,
        tick_step=0.2,
        dodge=False,
        palette=style.palette,
        order=["Prob", "Batch", "Delay", "FR", "D-FR", "AGE"],
    )


def figure12a(df: pd.DataFrame):
    data = df.query('Algorithm == "D-FR"')
    plt_wrapper.plt_box(
        data,
        y="Relative Promotion [LRU]",
        y_label="Promotions relative to LRU",
        x="Scale",
        x_label="Delay ratio",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure12a.pdf",
        marker_size=11,
        tick_step=0.2,
    )


def figure12b(df: pd.DataFrame):
    data = df.query('Algorithm == "D-FR"')
    plt_wrapper.plt_box(
        data,
        y="Relative Miss Ratio [LRU]",
        y_label="Miss ratio relative to LRU",
        x="Scale",
        x_label="Delay ratio",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure12b.pdf",
        marker_size=11,
    )


def figure12c(df: pd.DataFrame):
    data = df.query('Algorithm == "D-FR"')
    plt_wrapper.plt_box(
        data,
        y="Promotion Efficiency",
        y_label="Promotion efficiency",
        x="Scale",
        x_label="Delay ratio",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure12c.pdf",
        marker_size=11,
    )


def figure13a(df: pd.DataFrame):
    data = df.query('Algorithm == "AGE"')
    plt_wrapper.plt_box(
        data,
        y="Relative Promotion [LRU]",
        y_label="Promotions relative to LRU",
        x="Scale",
        x_label="Factor",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure13a.pdf",
        marker_size=11,
        tick_step=0.2,
    )


def figure13b(df: pd.DataFrame):
    data = df.query('Algorithm == "AGE"')
    plt_wrapper.plt_box(
        data,
        y="Relative Miss Ratio [LRU]",
        y_label="Miss ratio relative to LRU",
        x="Scale",
        x_label="Factor",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure13b.pdf",
        marker_size=11,
    )


def figure13c(df: pd.DataFrame):
    data = df.query('Algorithm == "AGE"')
    plt_wrapper.plt_box(
        data,
        y="Promotion Efficiency",
        y_label="Promotion efficiency",
        x="Scale",
        x_label="Factor",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure13c.pdf",
        marker_size=11,
    )


if __name__ == "__main__":
    import sys
    import os

    if len(sys.argv) < 2:
        raise ValueError("Missing required argument: figures to generate")

    os.makedirs("figures", exist_ok=True)
    df = pd.read_feather("./data/processed.feather")

    funcs = sys.argv[1:]
    for func in funcs:
        if func not in globals():
            raise ValueError(f"{func} is not available")
        globals()[func](df)
