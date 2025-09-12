import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt

df = (
    pl.read_parquet('data/2025-09-09_history.parquet')
    # Clean dataset
    .with_columns(
        pl.col('end_period_ts').max().over('ticker').alias('max_time_utc'),
        pl.col('result').replace({'yes': '1', 'no': '0'}).cast(pl.Int8),
        pl.col('end_period_ts').sub(pl.col('game_start_time_utc')).dt.total_minutes().alias("elapsed_time")
    )
    # Approximate beginning of 4th quarter
    .with_columns(
        pl.col('elapsed_time').quantile(.75, interpolation='lower').over('ticker').alias('trade_time')
    )
    # Filter to just prices after trade time
    .filter(
        pl.col('elapsed_time').ge(pl.col('trade_time'))
    )
    # Get trade price and time
    .sort('ticker', 'end_period_ts')
    .group_by('ticker', 'result')
    .agg(
        pl.col('yes_ask_close').first(),
        pl.col('end_period_ts').first(),
    )
    # Buy contracts between 60 and 99 dollars
    .filter(
        pl.col('yes_ask_close').is_between(60, 99)
    )
    # Calculate profit
    .with_columns(
        pl.when(pl.col('result').eq(1))
        .then(pl.lit(100).sub('yes_ask_close'))
        .when(pl.col('result').eq(0))
        .then(-pl.col('yes_ask_close'))
        .alias('profit')
    )
    .sort('profit', 'ticker')
)

print(df)

print(
    df
    .select(
        pl.col('yes_ask_close'),
        pl.col('profit'),
        pl.col('profit').truediv('yes_ask_close').mul(100).alias('return')
    )
    .select('return')
    .describe()
)

# Total profit
print(   
    df
     .select(
        pl.col('yes_ask_close').sum(),
        pl.col('profit').sum()
    )
    .with_columns(
        pl.col('profit').truediv('yes_ask_close').mul(100).alias('return')
    )   
)

# Total profit by day
profits = (   
    df
    .with_columns(
        pl.col('ticker').str.slice(12, 7).alias('day')
    )
    .group_by('day')
    .agg(
        pl.col('ticker').len().alias('n_games'),
        pl.col('yes_ask_close').sum().alias('value'),
        pl.col('profit').sum().alias('profit')
    )
    .with_columns(pl.col('profit').truediv('value').alias('return'))
    .sort('day')
    .with_columns(
        pl.col('return').add(1).cum_prod().sub(1).alias('cumulative_return')
    )
    .with_columns(
        pl.col('return', 'cumulative_return').mul(100)
    )
)

print(profits)

returns = profits['return'] / 100
sharpe = returns.mean() / returns.std() * (52 ** .5)

print(sharpe)


plt.figure(figsize=(10, 6))

sns.lineplot(profits, x='day', y='cumulative_return')

plt.title(f'Sharpe: {sharpe:.2f}')

plt.xlabel('Day')
plt.ylabel('Cumulative Product Return (%)')

plt.show()