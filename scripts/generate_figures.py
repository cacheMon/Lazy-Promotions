import pandas as pd
import matplotlib_wrapper as plt_wrapper
import style
import numpy as np

MISS_RATIO_VAL = "Relative Miss Ratio [LRU]"
MISS_RATIO_LABEL = "Miss ratio relative to LRU"

THROUGHPUT_VAL = "Relative Throughput [LRU]"
THROUGHPUT_LABEL = "Throughput relative to LRU"

# THROUGHPUT_VAL = "Throughput"
# THROUGHPUT_LABEL = "Throughput"


def figure1a(general: pd.DataFrame, scalability: pd.DataFrame):
    data = scalability.query(
        'Algorithm in ["Batch","Prob","Delay","FR"] and Thread == 16'
    )
    plt_wrapper.plt_scatter(
        data,
        MISS_RATIO_VAL,
        THROUGHPUT_VAL,
        x_label=MISS_RATIO_LABEL,
        y_label=THROUGHPUT_LABEL,
        hue="Algorithm",
        markers=style.markers,
        palette=style.palette,
        output_pdf="figures/figure1a.pdf",
    )
    pass


def figure1b(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query(
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
        x=MISS_RATIO_VAL,
        x_label=MISS_RATIO_LABEL,
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


def figure2a(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Prob"')
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


def figure2b(general: pd.DataFrame, scalability: pd.DataFrame):
    data = scalability.query('Algorithm == "Prob" and Thread == 16')
    plt_wrapper.plt_bar(
        data,
        y=THROUGHPUT_VAL,
        y_label=THROUGHPUT_LABEL,
        x="Scale",
        x_label="Prob",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure2b.pdf",
        invert=True,
    )


def figure2c(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Prob"')
    plt_wrapper.plt_box(
        data,
        y=MISS_RATIO_VAL,
        y_label=MISS_RATIO_LABEL,
        x="Scale",
        x_label="Prob",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure2c.pdf",
        marker_size=11,
        invert=True,
    )


def figure3a(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Batch"')
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


def figure3b(general: pd.DataFrame, scalability: pd.DataFrame):
    data = scalability.query('Algorithm == "Batch" and Thread == 16')
    plt_wrapper.plt_bar(
        data,
        y=THROUGHPUT_VAL,
        y_label=THROUGHPUT_LABEL,
        x="Scale",
        x_label="Batch size",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure3b.pdf",
    )


def figure3c(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Batch"')
    plt_wrapper.plt_box(
        data,
        y=MISS_RATIO_VAL,
        y_label=MISS_RATIO_LABEL,
        x="Scale",
        x_label="Batch size",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure3c.pdf",
        marker_size=11,
    )


def figure4a(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Delay"')
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


def figure4b(general: pd.DataFrame, scalability: pd.DataFrame):
    data = scalability.query('Algorithm == "Delay" and Thread == 16')
    plt_wrapper.plt_bar(
        data,
        y=THROUGHPUT_VAL,
        y_label=THROUGHPUT_LABEL,
        x="Scale",
        x_label="Delay",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure4b.pdf",
    )


def figure4c(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Delay"')
    plt_wrapper.plt_box(
        data,
        y=MISS_RATIO_VAL,
        y_label=MISS_RATIO_LABEL,
        x="Scale",
        x_label="Delay ratio",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure4c.pdf",
        marker_size=11,
    )


def figure5a(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "FR"')
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


def figure5b(general: pd.DataFrame, scalability: pd.DataFrame):
    data = scalability.query('Algorithm == "FR" and Thread == 16')
    plt_wrapper.plt_bar(
        data,
        y=THROUGHPUT_VAL,
        y_label=THROUGHPUT_LABEL,
        x="Scale",
        x_label="# Frequency bits",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure5b.pdf",
    )


def figure5c(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "FR"')
    plt_wrapper.plt_box(
        data,
        y=MISS_RATIO_VAL,
        y_label=MISS_RATIO_LABEL,
        x="Bit",
        x_label="# Frequency bits",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure5c.pdf",
        marker_size=11,
    )


def figure6a(general: pd.DataFrame, scalability: pd.DataFrame):
    data = scalability.query('Algorithm == "Random" and Thread == 16')
    plt_wrapper.plt_bar(
        data,
        y=THROUGHPUT_VAL,
        y_label=THROUGHPUT_LABEL,
        x="Scale",
        x_label="Sample size",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure6a.pdf",
    )


def figure6b(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Random"')
    plt_wrapper.plt_box(
        data,
        y=MISS_RATIO_VAL,
        y_label=MISS_RATIO_LABEL,
        x="Scale",
        x_label="Sample size",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure6b.pdf",
        marker_size=11,
    )


def figure7a(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Prob"')
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


def figure7b(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Batch"')
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


def figure7c(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Delay"')
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


def figure7d(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "FR"')
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


def figure8a(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "ARC"')
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


def figure8b(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "ARC"')
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


def figure8c(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "TwoQ"')
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


def figure8d(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "TwoQ"')
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


def figure9a(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Belady-RandomLRU"')
    data.loc[data["Scale"] > 10, "Scale"] = np.inf

    def format_scale(v):
        if np.isinf(v):
            return "inf"
        return str(int(v))

    data["Scale"] = data["Scale"].apply(format_scale)
    plt_wrapper.plt_box(
        data,
        y=MISS_RATIO_VAL,
        y_label=MISS_RATIO_LABEL,
        x="Scale",
        x_label="Factor",
        x_size=12,
        tick_step=0.05,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure9a.pdf",
        marker_size=11,
    )


def figure9b(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Belady-Random"')
    data.loc[data["Scale"] > 10, "Scale"] = np.inf

    def format_scale(v):
        if np.isinf(v):
            return "inf"
        return str(int(v))

    data["Scale"] = data["Scale"].apply(format_scale)
    plt_wrapper.plt_box(
        data,
        y=MISS_RATIO_VAL,
        y_label=MISS_RATIO_LABEL,
        x="Scale",
        x_label="Factor",
        x_size=12,
        tick_step=0.05,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure9b.pdf",
        marker_size=11,
    )


def figure9c(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Belady-Random"')
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


def figure10a(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Offline-FR"')
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


def figure10b(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Offline-FR"')
    plt_wrapper.plt_box(
        data,
        y=MISS_RATIO_VAL,
        y_label=MISS_RATIO_LABEL,
        x="Scale",
        x_label="# Iteration",
        x_size=12,
        tick_step=0.02,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure10b.pdf",
        marker_size=11,
    )


def figure10c(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "Offline-FR"')
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


def figure11a(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query(
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


def figure11b(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query(
        '(Algorithm == "Delay" and Scale == 0.2) or'
        '(Algorithm == "Prob" and Scale == 0.4) or'
        '(Algorithm == "Batch" and Scale == 0.5) or'
        '(Algorithm == "FR" and Bit == 1) or'
        '(Algorithm == "D-FR" and Scale == 0.05 and Bit == 1) or'
        '(Algorithm == "AGE" and Scale == 0.5)'
    )
    plt_wrapper.plt_box(
        data,
        y=MISS_RATIO_VAL,
        y_label=MISS_RATIO_LABEL,
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


def figure11c(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query(
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


def figure12a(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "D-FR"')
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


def figure12b(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "D-FR"')
    plt_wrapper.plt_box(
        data,
        y=MISS_RATIO_VAL,
        y_label=MISS_RATIO_LABEL,
        x="Scale",
        x_label="Delay ratio",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure12b.pdf",
        marker_size=11,
    )


def figure12c(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "D-FR"')
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


def figure13a(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "AGE"')
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


def figure13b(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "AGE"')
    plt_wrapper.plt_box(
        data,
        y=MISS_RATIO_VAL,
        y_label=MISS_RATIO_LABEL,
        x="Scale",
        x_label="Factor",
        x_size=12,
        hue="Algorithm",
        palette=["lightblue"],
        output_pdf="figures/figure13b.pdf",
        marker_size=11,
    )


def figure13c(general: pd.DataFrame, scalability: pd.DataFrame):
    data = general.query('Algorithm == "AGE"')
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
    general = pd.read_feather("./data/processed.feather")
    scalability = None
    if os.path.exists("./data/processed_scalability.feather"):
        scalability = pd.read_feather("./data/processed_scalability.feather")

    funcs = sys.argv[1:]
    for func in funcs:
        if func not in globals():
            raise ValueError(f"{func} is not available")
        globals()[func](general, scalability)
