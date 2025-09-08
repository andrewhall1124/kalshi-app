from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/andrewhall1124/kalshi-app.git",
        entrypoint="src/flows/open_markets_flow.py:main",
    ).deploy(
        name="open-markets-flow",
        work_pool_name="my-managed-pool",
    )