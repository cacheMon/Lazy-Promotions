import pandas as pd
import matplotlib_wrapper as plt_wrap
import style
import numpy as np
from scipy.spatial import cKDTree

throughput = pd.read_csv(
    "../../lazy_promotions/scalability_results/averaged_results_all.csv"
)
throughput["algorithm"] = throughput["algorithm"].apply(lambda s: s.replace("LRU_", ""))

# print(throughput)

# throughput = throughput.query(
#     "(algorithm == 'FR' and param == 1) or (algorithm == 'Prob' and param == 0.4) or (algorithm == 'Batch' and param == 0.5) or (algorithm == 'Delay' and param == 0.2)"
# )
# print(throughput)

df = pd.read_feather("./data/processed.feather")

scale_data = df.query("Algorithm in ['Prob','Batch','Delay','Random']")
fr = df.query("Algorithm in ['FR']")

scale_mean = df.groupby(["Algorithm", "Scale"]).mean(numeric_only=True).reset_index()
fr_mean = df.groupby(["Algorithm", "Bit"]).mean(numeric_only=True).reset_index()

scale_combined = pd.merge(
    throughput,
    scale_mean,
    left_on=["algorithm", "param"],
    right_on=["Algorithm", "Scale"],
    how="inner",
)

fr_combined = pd.merge(
    throughput,
    fr_mean,
    left_on=["algorithm", "param"],
    right_on=["Algorithm", "Bit"],
    how="inner",
)

print(scale_combined)
print(fr_combined)

overall = pd.concat([scale_combined, fr_combined], ignore_index=True)
overall.to_csv("idk.csv")


def thin_by_distance(df, x_col, y_col, marker_col, min_dist=0.1):
    keep_idx = []
    for mtype, group in df.groupby(marker_col):
        pts = group[[x_col, y_col]].values
        tree = cKDTree(pts)
        seen = set()
        for i, pt in enumerate(pts):
            if i not in seen:
                keep_idx.append(group.index[i])
                neighbors = tree.query_ball_point(pt, min_dist)
                seen.update(neighbors)
    return df.loc[keep_idx]


overall = thin_by_distance(
    overall,
    "Relative Miss Ratio [LRU]",
    "thr_relative_to_lru",
    "algorithm",
    min_dist=0.5,
)

plt_wrap.plt_scatter(
    overall,
    "Relative Miss Ratio [LRU]",
    "thr_relative_to_lru",
    x_label="Miss ratio relative to LRU",
    y_label="Throughput relative to LRU",
    hue="algorithm",
    markers=style.markers,
    palette=[
        "#eff3ff",
        "#bdd7e7",
        "#6baed6",
        "#3182bd",
        "#08519c",
    ],
    order=["Batch", "Prob", "Random", "Delay", "FR"],
    output_pdf="scalability.pdf",
    alpha=0.8,
)
