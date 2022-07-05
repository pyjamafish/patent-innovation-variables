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


def get_citation_df():
    return pl.read_csv(
        file="patent_analysis/resources/uspatentcitation.tsv",
        sep="\t",
        columns=["patent_id", "citation_id"],
        dtypes={
            "patent_id": pl.Utf8,
            "citation_id": pl.Utf8,
        }
    )


print(get_citation_df())
