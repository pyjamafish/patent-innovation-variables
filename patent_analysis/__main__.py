import polars as pl
from importlib import resources
from datetime import datetime

RESOURCE_PATH = resources.files("patent_analysis.resources.mini")


def get_patent_lf() -> pl.LazyFrame:
    return (
        pl.scan_csv(
            file=str(RESOURCE_PATH.joinpath("patent.tsv")),
            sep="\t",
            dtypes={"date": pl.Date}
        )
        .select(["id", "date"])
        .filter(
            pl.col("date") >= pl.lit(datetime(1999, 1, 1))
        )
        .filter(
            pl.col("date") <= pl.lit(datetime(2019, 12, 31))
        )
    )


def get_citation_lf() -> pl.LazyFrame:
    return pl.scan_csv(
        file=str(RESOURCE_PATH.joinpath("uspatentcitation.tsv")),
        sep="\t",
    )


print(
    get_patent_lf()
    .join(get_citation_lf(), left_on="id", right_on="patent_id")
    .groupby("citation_id")
    .agg(
        [
            pl.col("id").list(),
            pl.col("date").list()
        ]
    )
    .collect()
)
