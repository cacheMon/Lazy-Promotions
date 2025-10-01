import pandas as pd
import matplotlib_wrapper as plt_wrap
import style

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

plt_wrap.plt_scatter(
    overall,
    "Relative Miss Ratio [LRU]",
    "thr_relative_to_lru",
    x_label="Miss ratio relative to LRU",
    y_label="Throughput relative to LRU",
    hue="algorithm",
    markers=style.markers,
    palette=style.palette,
    output_pdf="scalability.pdf",
)
