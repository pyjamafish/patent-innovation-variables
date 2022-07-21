import polars as pl
from patent_analysis import citations


def test_citation(
    citations_patent_path,
    citations_sample_path,
    citations_uspatentcitation_path,
    citations_expected_output_path,
    tmp_path
) -> None:
    lf = citations.get_sample_output_from_universe_output(
        citations.get_output_universe_lf(
            patent_path=citations_patent_path,
            citation_path=citations_uspatentcitation_path
        ),
        sample_path=citations_sample_path
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
