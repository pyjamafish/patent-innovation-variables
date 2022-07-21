import polars as pl
from importlib import resources

from patent_analysis import SAMPLE_EARLIEST_DATE, SAMPLE_LATEST_DATE, SAMPLE_5_YEAR_CUTOFF

RESOURCE_PATH = resources.files("patent_analysis.data.citations")


def get_patent_lf(path) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            file=path,
            sep="\t",
            dtypes={"id": pl.Utf8, "date": pl.Date}
        )
        .select(["id", "date"])
        .filter(
            pl.col("date") >= SAMPLE_EARLIEST_DATE
        )
    )


def get_sample_lf(path) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            file=path,
            dtypes={"patent_num": pl.Utf8}
        )
        .with_column(pl.col('issue_date').str.strptime(pl.Date, fmt='%m/%d/%Y').alias("date"))
        .select(["patent_num", "date"])
        .rename({"patent_num": "id"})
    )


def get_citation_lf(path) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            file=path,
            sep="\t",
            dtypes={"patent_id": pl.Utf8, "citation_id": pl.Utf8}
        )
        .select(["patent_id", "citation_id"])
    )


def get_citations_count(years: int) -> pl.Expr:
    return (
        pl.col("citing_patent_issue_date") <= pl.col("cited_patent_issue_date").first().dt.offset_by(f"{years}y")
    ).sum().alias(f"citations_{years}_years")


def get_output_universe_lf(patent_path, citation_path) -> pl.LazyFrame:
    return (
        get_citation_lf(citation_path)
        .rename(
            {
                "patent_id": "citing_patent",
                "citation_id": "cited_patent",
            }
        )
        .join(get_patent_lf(patent_path), left_on="cited_patent", right_on="id")
        .rename({"date": "cited_patent_issue_date"})
        .filter(
            pl.col("cited_patent_issue_date") <= SAMPLE_LATEST_DATE
        )
        .join(get_patent_lf(patent_path), left_on="citing_patent", right_on="id")
        .rename({"date": "citing_patent_issue_date"})
        .groupby("cited_patent")
        .agg(
            [
                pl.col("cited_patent_issue_date").first(),
                get_citations_count(3),
                get_citations_count(5)
            ]
        )
        .with_column(
            pl.when(pl.col("cited_patent_issue_date") > SAMPLE_5_YEAR_CUTOFF)
            .then(pl.lit(None))
            .otherwise(pl.col("citations_5_years"))
            .alias("citations_5_years")
        )
    )


def get_sample_output_from_universe_output(universe_output: pl.LazyFrame, sample_path: str) -> pl.LazyFrame:
    return universe_output.filter(
        pl.col("cited_patent").is_in(
            get_sample_lf(sample_path).select("id").collect().get_column("id")
        )
    )


def main():
    output_universe = get_output_universe_lf(
        patent_path=f"{RESOURCE_PATH}/patent.tsv",
        citation_path=f"{RESOURCE_PATH}/uspatentcitation.tsv"
    ).collect()
    output_universe.write_csv(file=f"{RESOURCE_PATH}/output_universe.tsv", sep="\t")

    output_sample = get_sample_output_from_universe_output(
        output_universe.lazy(),
        f"{RESOURCE_PATH}/sample.csv"
    ).collect()
    output_sample.write_csv(file=f"{RESOURCE_PATH}/output_sample.tsv", sep="\t")


if __name__ == '__main__':
    main()
