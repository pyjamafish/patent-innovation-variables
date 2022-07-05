import polars as pl


def get_patent_df():
    return pl.read_csv(
        file="patent_analysis/resources/patent.tsv",
        sep="\t",
        columns=["id", "number", "date"],
        dtypes={
            "id": pl.Utf8,
            "number": pl.Utf8,
            "date": pl.Date
        }
    )


print(get_patent_df())
