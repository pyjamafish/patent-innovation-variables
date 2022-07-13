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
    )


def get_citation_lf() -> pl.LazyFrame:
    return (
        pl.scan_csv(
            file=str(RESOURCE_PATH.joinpath("uspatentcitation.tsv")),
            sep="\t",
        )
        .select(["patent_id", "citation_id"])
    )


def get_citation_count(years: int) -> pl.Expr:
    return (
        pl.col("citing_patent_issue_date") <= pl.col("cited_patent_issue_date").first().dt.offset_by(f"{years}y")
    ).sum().alias(f"citations_{years}_years")


print(
    get_citation_lf()
    .rename(
        {
            "patent_id": "citing_patent",
            "citation_id": "cited_patent",
        }
    )
    .join(get_patent_lf(), left_on="cited_patent", right_on="id")
    .rename({"date": "cited_patent_issue_date"})
    .filter(
        pl.col("cited_patent_issue_date") <= pl.lit(datetime(2019, 12, 31))
    )
    .join(get_patent_lf(), left_on="citing_patent", right_on="id")
    .rename({"date": "citing_patent_issue_date"})
    .groupby("cited_patent")
    .agg(
        [
            get_citation_count(3),
            get_citation_count(5)
        ]
    )
    .collect()
)
