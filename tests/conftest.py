import pytest
from importlib import resources


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
