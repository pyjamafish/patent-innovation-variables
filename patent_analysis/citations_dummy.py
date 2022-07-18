import pandas as pd
import numpy as np

from importlib import resources
from datetime import datetime

CITATIONS_COUNT_PATH = resources.files("patent_analysis.data.citations")
RESOURCE_PATH = resources.files("patent_analysis.data.citations_dummy")


def get_subclass_df(path=f"{RESOURCE_PATH}/ipcr.tsv") -> pd.DataFrame:
    return (
        pd.read_csv(
            path,
            sep="\t",
            dtype={
                "patent_id": str,
                "section": str,
                "ipc_class": str,
                "subclass": str
            },
            usecols=["patent_id", "section", "ipc_class", "subclass"]
        )
    )


def get_citations_count_df(path=f"{CITATIONS_COUNT_PATH}/output.tsv") -> pd.DataFrame:
    return (
        pd.read_csv(
            path,
            sep="\t",
            dtype={
                "cited_patent": str,
                "citations_3_years": "UInt32",
                "citations_5_years": "UInt32",
            },
            parse_dates=True
        )
    )


def get_output_df(
        citations_count_path=f"{CITATIONS_COUNT_PATH}/output.tsv",
        subclass_path=f"{RESOURCE_PATH}/ipcr.tsv"
) -> pd.DataFrame:
    return (
        get_citations_count_df(path=citations_count_path)
        .set_index("cited_patent")
        .join(get_subclass_df(path=subclass_path).set_index("patent_id"))
        .groupby(
            [
                "cited_patent_issue_date",
                "section",
                "ipc_class",
                "subclass"
            ]
        )
        ["citations_3_years"]
        .rank(pct=True)
    )


def main():
    pass


if __name__ == '__main__':
    main()
