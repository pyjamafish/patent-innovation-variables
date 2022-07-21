from importlib import resources

import polars as pl
import pytest

from patent_analysis import citations


def get_filename_in_citations_package(basename: str) -> str:
    return f"{resources.files('tests.fixtures.citations.citations')}/{basename}"


@pytest.fixture()
def patent_path():
    return get_filename_in_citations_package("patent.tsv")


@pytest.fixture()
def sample_path():
    return get_filename_in_citations_package("sample.csv")


@pytest.fixture()
def uspatentcitation_path():
    return get_filename_in_citations_package("uspatentcitation.tsv")


@pytest.fixture()
def expected_output_sample_path():
    return get_filename_in_citations_package("output_sample.tsv")


def test_citation(
    patent_path,
    sample_path,
    uspatentcitation_path,
    expected_output_sample_path,
) -> None:
    lf = (
        citations.get_output_universe_lf(
            patent_path=patent_path,
            citation_path=uspatentcitation_path
        )
        .filter(citations.in_sample(sample_path=sample_path))
    )
    df_actual = lf.collect().sort(by="cited_patent")

    df_expected = (
        pl.read_csv(
            expected_output_sample_path,
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
