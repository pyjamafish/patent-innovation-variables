from importlib import resources

import polars as pl
import pytest

from patent_analysis import citations_dummy


def get_fixture_filename(filename: str) -> str:
    return f"{resources.files('tests.fixtures.citations_dummy')}/{filename}"


@pytest.fixture()
def output_universe_path():
    return get_fixture_filename("citations/output_universe.tsv")


@pytest.fixture()
def sample_path():
    return get_fixture_filename("citations/sample.csv")


@pytest.fixture()
def ipcr_path():
    return get_fixture_filename("citations_dummy/ipcr.tsv")


@pytest.fixture()
def expected_output_path():
    return get_fixture_filename("citations_dummy/output.tsv")


def test_citations_dummy(
    output_universe_path,
    sample_path,
    ipcr_path,
    expected_output_path,
) -> None:
    lf = (
        citations_dummy.get_output_lf(
            citations_count_path=output_universe_path,
            sample_path=sample_path,
            subclass_path=ipcr_path
        )
    )
    df_actual = lf.sort(by="cited_patent").collect()

    df_expected = (
        pl.read_csv(
            expected_output_path,
            sep="\t",
            dtypes={
                "citations_3_years_percentile_95": pl.UInt8,
                "citations_5_years_percentile_95": pl.UInt8,
                "citations_3_years_percentile_99": pl.UInt8,
                "citations_5_years_percentile_99": pl.UInt8
            }
        )
        .sort(by="cited_patent")
    )

    pass

    assert(df_actual.frame_equal(df_expected))
