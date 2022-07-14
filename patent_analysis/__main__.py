import polars as pl
from importlib import resources
from datetime import datetime


RESOURCE_PATH = resources.files("patent_analysis.resources.stripped")


def get_patent_lf() -> pl.LazyFrame:
    return (
        pl.scan_csv(
            file=str(RESOURCE_PATH.joinpath("patent.tsv")),
            sep="\t",
            dtypes={"id": pl.Utf8, "date": pl.Date}
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
            dtypes={"patent_id": pl.Utf8, "citation_id": pl.Utf8}
        )
        .select(["patent_id", "citation_id"])
    )


def get_citation_count(years: int) -> pl.Expr:
    return (
        pl.col("citing_patent_issue_date") <= pl.col("cited_patent_issue_date").first().dt.offset_by(f"{years}y")
    ).sum().alias(f"citations_{years}_years")


def get_output_lf() -> pl.LazyFrame:
    return (
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
            pl.col("cited_patent_issue_date") <= pl.lit(datetime(2018, 12, 31))
        )
        .join(get_patent_lf(), left_on="citing_patent", right_on="id")
        .rename({"date": "citing_patent_issue_date"})
        .groupby("cited_patent")
        .agg(
            [
                pl.col("cited_patent_issue_date").first(),
                get_citation_count(3),
                get_citation_count(5)
            ]
        )
        .with_column(
            pl.when(pl.col("cited_patent_issue_date") > datetime(2016, 12, 31))
            .then(pl.lit(None))
            .otherwise(pl.col("citations_5_years"))
            .alias("citations_5_years")
        )
    )


def main():
    lf = get_output_lf()
    lf.collect().write_csv(file=str(RESOURCE_PATH.joinpath("output.tsv")), sep="\t")


if __name__ == '__main__':
    main()
