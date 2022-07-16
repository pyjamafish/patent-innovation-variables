import polars as pl
from patent_analysis import citation


def test_citation(
    citation_patent_path,
    citation_sample_path,
    citation_uspatentcitation_path,
    citation_expected_output_path,
    tmp_path
) -> None:
    lf = citation.get_output_lf(
        patent_path=citation_patent_path,
        sample_path=citation_sample_path,
        citation_path=citation_uspatentcitation_path
    )
    df_actual = lf.collect().sort(by="cited_patent")

    df_expected = (
        pl.read_csv(
            citation_expected_output_path,
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
