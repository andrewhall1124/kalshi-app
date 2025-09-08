from src.flows.open_markets_flow import open_markets_flow

if __name__ == "__main__":
    open_markets_flow.serve(
        name="open-markets-flow",
        cron="* * * * *"
    )
