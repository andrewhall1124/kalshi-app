import os
from typing import Any, List, Optional, Tuple

import psycopg2
from dotenv import load_dotenv
import sqlalchemy

load_dotenv(override=True)

def get_engine():
    return sqlalchemy.create_engine(os.getenv("DATABASE_URL"))

def get_database_uri():
    return os.getenv("DATABASE_URL")

def execute_sql(query: str, params: Optional[Tuple] = None, fetch: bool = False) -> Optional[List[Tuple[Any, ...]]]:
    """
    Execute a SQL query against the Neon database.
    
    Args:
        query: SQL query string
        params: Optional tuple of parameters for the query
        fetch: Whether to fetch and return results (True for SELECT, False for INSERT/UPDATE/DELETE)
    
    Returns:
        List of tuples containing query results if fetch=True, None otherwise
    
    Raises:
        Exception: If database connection or query execution fails
    """
    conn_string = os.getenv("DATABASE_URL")
    
    if not conn_string:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                
                if fetch:
                    return cur.fetchall()
                else:
                    conn.commit()
                    return None
                    
    except Exception as e:
        raise Exception(f"Database operation failed: {str(e)}")
