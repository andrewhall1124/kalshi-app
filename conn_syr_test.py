import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt

df = pl.read_parquet("data/2025-09-09_history.parquet")

tickers = [
    'KXNCAAFGAME-25SEP06CONNSYR-SYR',
    'KXNCAAFGAME-25SEP06CONNSYR-CONN'
]

sample = (
    df
    .filter(
        pl.col('ticker').is_in(tickers)
    )
    .with_columns(
        pl.col('end_period_ts', 'game_start_time_utc', 'game_end_time_utc').dt.convert_time_zone('America/Denver'),
        pl.mean_horizontal('yes_bid_close', 'yes_ask_close').alias('yes_mid_close'),
        pl.col('ticker').str.split('-').list.get(2).alias('short_ticker')
    )
    .sort('end_period_ts')
)

print(sample)

print(sample['ticker'].last())

plt.figure(figsize=(10, 6))

sns.lineplot(sample, x='end_period_ts', y='yes_mid_close', hue='short_ticker')

plt.title("25SEP06CONNSYR")

plt.xlabel(None)
plt.ylabel('Yes Close')

plt.legend(title='Ticker')

plt.show()