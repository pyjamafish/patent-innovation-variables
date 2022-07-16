import pytest
from importlib import resources


def get_filename_in_citation_package(basename: str) -> str:
    return f"{resources.files('tests.fixtures.citation')}/{basename}"


@pytest.fixture()
def citation_patent_path():
    return get_filename_in_citation_package("patent.tsv")


@pytest.fixture()
def citation_sample_path():
    return get_filename_in_citation_package("sample.csv")


@pytest.fixture()
def citation_uspatentcitation_path():
    return get_filename_in_citation_package("uspatentcitation.tsv")


@pytest.fixture()
def citation_expected_output_path():
    return get_filename_in_citation_package("output.tsv")
