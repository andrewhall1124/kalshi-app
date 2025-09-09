import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
from rich import print

df = (
    pl.read_parquet("data/2025-09-08_history.parquet")
    .with_columns(
        pl.mean_horizontal('yes_ask_close', 'yes_bid_close').alias('yes_mid_close')
    )
)

print(df)

# df_filter = (
#     df
#     .filter(
#         pl.col('yes_mid_close').ne(50)
#     )
#     .sort('ticker', 'end_period_ts')
# )

all_tickers = df['ticker'].unique().sort().to_list()

n_plots = 18
for i in range(n_plots):
    start_idx = i * 20
    end_idx = (i + 1) * 20
    tickers = all_tickers[start_idx:end_idx]
    
    fig, axes = plt.subplots(4, 5, figsize=(13, 10))
    
    for ticker, ax in zip(tickers, axes.flatten()):
        subset = df.filter(pl.col('ticker').eq(ticker)).sort('end_period_ts')
        ax.plot(subset['end_period_ts'], subset['yes_mid_close'])
        ax.set_ylim(0, 100)
        ax.set_xticks([])
        ax.set_title("-".join(ticker.split("-")[1:3]))
    
    # Hide unused subplots
    for j in range(len(tickers), len(axes.flatten())):
        axes.flatten()[j].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(f'charts/plot_{i+1}.png', dpi=300, bbox_inches='tight')
    plt.close()
