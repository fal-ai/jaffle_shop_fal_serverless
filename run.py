from fal_serverless import isolated, sync_dir

sync_dir("/Users/gorkemyurtseven/dev/fal-serverless-duckdb/jaffle_shop", "/data/jaffle_shop")
sync_dir("/Users/gorkemyurtseven/dev/fal-serverless-duckdb/profiles", "/data/profiles")

@isolated(requirements=["fal", "dbt-duckdb==1.5.0rc1"])
def seed():
    from dbt.cli.main import dbtRunner
    
    runner = dbtRunner()
    
    cli_args = ["seed", 
            "--project-dir", 
            "/data/jaffle_shop", 
            "--profiles-dir", 
            "/data/profiles"]

    runner.invoke(cli_args)

@isolated(requirements=["fal", "dbt-duckdb==1.5.0rc1"])
def run():

    from dbt.cli.main import dbtRunner
    
    runner = dbtRunner()
    
    cli_args = ["run", 
            "--project-dir", 
            "/data/jaffle_shop", 
            "--profiles-dir", 
            "/data/profiles"]

    runner.invoke(cli_args)

@isolated(requirements=["duckdb"], serve=True)
def save_event(event):
    import duckdb
    import json

    con = duckdb.connect("/data/duck.db")
    con.sql("CREATE TABLE IF NOT EXISTS events (j JSON);")
    query = f"INSERT INTO events VALUES('{json.dumps(event)}')"
    con.sql(query)
    return

@isolated(requirements=["duckdb", "buenavista[duckdb]", "uvicorn"], exposed_port=8080)
def query():
    """
    This helps you query the datawarehouse. Submit SQL statements here.
    """
    import uvicorn
    import duckdb
    from fastapi import FastAPI
    from buenavista.http import main
    from buenavista.backends.duckdb import DuckDBConnection

    from buenavista import bv_dialects, rewrite

    rewriter = rewrite.Rewriter(bv_dialects.BVTrino(), bv_dialects.BVDuckDB())

    db = duckdb.connect("/data/dbt.duckdb")
    app = FastAPI()

    main.quacko(app, DuckDBConnection(db), rewriter)
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")

