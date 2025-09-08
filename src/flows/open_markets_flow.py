import polars as pl
from src.shared.kalshi import kalshi_client
import datetime as dt
import src.shared.database as db
from prefect import task, flow

@task
def get_open_markets() -> pl.DataFrame:
    series_ticker = 'KXNCAAFGAME'
    limit = 1000
    status = 'open'

    response = kalshi_client.get_markets(
        series_ticker=series_ticker, 
        limit=limit, 
        status=status
    )

    df = (
        pl.from_dicts(response.markets)
        .with_columns(
            pl.lit(dt.datetime.now(dt.timezone.utc)).alias("timestamp")
        )
    )

    return df

@flow
def open_markets_flow() -> None:
    # Create table
    with open("src/flows/sql/create_markets.sql", "r") as file:
        db.execute_sql(file.read())

    # Get open markets
    markets = get_open_markets()

    # Stage data
    now = int(dt.datetime.now(dt.timezone.utc).timestamp())
    stage_table = f"markets_{now}"
    markets.write_database(
        table_name=stage_table,
        connection=db.get_database_uri(),
        if_table_exists='replace'
    )

    # Merge data
    with open("src/flows/sql/merge_markets.sql", "r") as file:
        db.execute_sql(file.read().format(stage_table=stage_table))

    # Drop stage table
    db.execute_sql(f"DROP TABLE {stage_table};")
