# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb==1.4.1",
#     "marimo",
#     "polars==1.35.1",
#     "typer==0.20.0",
# ]
# ///

import marimo

__generated_with = "0.17.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import duckdb
    return (mo,)


@app.cell
def _(mo):
    from typer import Typer

    typer_app = Typer()

    @typer_app.command()
    def visualize(name: str):
        print(f"Visualizing {name}")

    @typer_app.command()
    def mart_schema(table: str):
        print(f"Schema for table {table}")

    if mo.app_meta().mode == "script":
        typer_app()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
