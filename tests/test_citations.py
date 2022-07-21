from importlib import resources

import polars as pl
import pytest

from patent_analysis import citations


def get_filename_in_citations_package(basename: str) -> str:
    return f"{resources.files('tests.fixtures.citations')}/{basename}"


@pytest.fixture()
def citations_patent_path():
    return get_filename_in_citations_package("patent.tsv")


@pytest.fixture()
def citations_sample_path():
    return get_filename_in_citations_package("sample.csv")


@pytest.fixture()
def citations_uspatentcitation_path():
    return get_filename_in_citations_package("uspatentcitation.tsv")


@pytest.fixture()
def citations_expected_output_path():
    return get_filename_in_citations_package("output.tsv")


def test_citation(
    citations_patent_path,
    citations_sample_path,
    citations_uspatentcitation_path,
    citations_expected_output_path,
    tmp_path
) -> None:
    lf = (
        citations.get_output_universe_lf(
            patent_path=citations_patent_path,
            citation_path=citations_uspatentcitation_path
        )
        .filter(citations.in_sample(sample_path=citations_sample_path))
    )
    df_actual = lf.collect().sort(by="cited_patent")

    df_expected = (
        pl.read_csv(
            citations_expected_output_path,
            sep="\t",
            dtypes={
                "cited_patent_issue_date": pl.Date,
                "citations_3_years": pl.UInt32,
                "citations_5_years": pl.UInt32,
            }
        )
        .sort(by="cited_patent")
    )
    assert(df_actual.frame_equal(df_expected))
