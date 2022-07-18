import polars as pl

from importlib import resources

CITATIONS_COUNT_PATH = resources.files("patent_analysis.data.citations")
RESOURCE_PATH = resources.files("patent_analysis.data.citations_dummy")


def get_subclass_lf(path=f"{RESOURCE_PATH}/ipcr.tsv") -> pl.LazyFrame:
    return (
        pl.scan_csv(
            file=path,
            sep="\t",
            dtypes={
                "patent_id": pl.Utf8,
                "section": pl.Utf8,
                "ipc_class": pl.Utf8,
                "subclass": pl.Utf8
            }
        )
        .select(
            ["patent_id", "section", "ipc_class", "subclass"]
        )
        .unique()
    )


def get_citations_count_lf(path=f"{CITATIONS_COUNT_PATH}/output.tsv") -> pl.LazyFrame:
    return (
        pl.scan_csv(
            path,
            sep="\t",
            dtypes={
                "cited_patent": pl.Utf8,
                "cited_patent_issue_date": pl.Date,
                "citations_3_years": pl.UInt32,
                "citations_5_years": pl.UInt32,
            }
        )
    )


def get_output_lf(
        citations_count_path=f"{CITATIONS_COUNT_PATH}/output.tsv",
        subclass_path=f"{RESOURCE_PATH}/ipcr.tsv"
) -> pl.LazyFrame:
    return (
        get_citations_count_lf(path=citations_count_path)
        .join(get_subclass_lf(path=subclass_path), left_on="cited_patent", right_on="patent_id")
        .with_column(
            pl.col("citations_3_years")
            .rank()
            .over(
                [
                    "cited_patent_issue_date",
                    "section",
                    "ipc_class",
                    "subclass"
                ]
            )
            .alias("rank")
        )
    )


def main():
    pass


if __name__ == '__main__':
    main()
