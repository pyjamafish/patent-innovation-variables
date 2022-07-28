"""
This file generates all the fixtures
for citations_dummy besides citations_dummy/output.tsv.
"""

import pandas as pd
import polars as pl
import numpy as np
from scipy.stats import skewnorm

from datetime import datetime
from dataclasses import dataclass
from typing import Optional

COHORT_YEARS = {
    "A": 1999,
    "B": 1999,
    "C": 2017,
    "D": 2017,
    "E": 2000,
    "F": 2000
}

GENERATED_PATENTS_PER_COHORT = 98

SEED = 521


@dataclass
class CitationPercentiles:
    percentile_3_years: int
    percentile_5_years: Optional[int]


@dataclass
class SamplePatent:
    issue_date: datetime
    citation_percentiles: dict[str, CitationPercentiles]

    def __str__(self):
        return "/".join(
            [
                f"{cohort}-{citation_percentile.percentile_3_years}-{citation_percentile.percentile_5_years}"
                for cohort, citation_percentile in self.citation_percentiles.items()
            ]
        )


SAMPLE = [
    SamplePatent(
        datetime(1999, 3, 22),
        {"A": CitationPercentiles(60, 60), "B": CitationPercentiles(94, 94)}
    ),
    SamplePatent(
        datetime(1999, 4, 7),
        {"A": CitationPercentiles(95, 94), "B": CitationPercentiles(20, 20)}
    ),
    SamplePatent(
        datetime(2017, 11, 17),
        {"C": CitationPercentiles(99, None), "D": CitationPercentiles(95, None)}
    ),
    SamplePatent(
        datetime(2017, 3, 29),
        {"C": CitationPercentiles(94, None), "D": CitationPercentiles(95, None)}
    ),
    SamplePatent(
        datetime(2000, 10, 29),
        {"E": CitationPercentiles(32, 32), "F": CitationPercentiles(99, 99)}
    ),
    SamplePatent(
        datetime(2000, 10, 25),
        {"E": CitationPercentiles(94, 98), "F": CitationPercentiles(60, 60)}
    )

]


def skewed_distribution(skewness):
    d = skewnorm.rvs(a=skewness, size=GENERATED_PATENTS_PER_COHORT, random_state=np.random.default_rng(SEED))
    d = d - min(d)
    d = d / max(d)
    d = d * 1000
    return d.astype(int)


def uniform_distribution():
    generator = np.random.default_rng(SEED)

    return generator.uniform(
        low=0,
        high=1000,
        size=GENERATED_PATENTS_PER_COHORT
    )


def generate_cohort_df(prefix: str, distribution) -> pl.DataFrame:
    generator = np.random.default_rng(SEED)

    year = COHORT_YEARS[prefix]
    random_dates = datetime(year, 1, 1) + pd.to_timedelta(
        generator.integers(
            low=0,
            high=365,
            size=GENERATED_PATENTS_PER_COHORT
        ),
        unit="D"
    )

    cited_patent = [
        f"{prefix}{suffix}"
        for suffix in range(1, GENERATED_PATENTS_PER_COHORT + 1)
    ]

    return pl.DataFrame(
        {
            "cited_patent": cited_patent,
            "cited_patent_issue_date": random_dates,
            "citations_3_years": distribution,
            "citations_5_years": (distribution * 1.2).astype(int)
        }
    ).with_column(pl.col("cited_patent_issue_date").cast(pl.Date))


def get_sample_citations_3_years(generated_cohort_dfs) -> pl.Series:
    pass


def get_sample_citations_5_years(generated_cohort_dfs) -> pl.Series:
    pass


def get_output_sample_df(generated_cohort_dfs: dict[str, pl.DataFrame]):
    df = pl.DataFrame(
        {
            "cited_patent": [str(sample_patent) for sample_patent in SAMPLE],
            "cited_patent_issue_date": [sample_patent.issue_date for sample_patent in SAMPLE],
        }
    )

    return (
        df.with_column(
            get_sample_citations_3_years(generated_cohort_dfs)
        )
        .with_column(
            get_sample_citations_5_years(generated_cohort_dfs)
        )
    )


def generate_output_universe_df() -> pl.DataFrame:
    generated_cohort_dfs = {
        "A": generate_cohort_df("A", skewed_distribution(-10)),
        "B": generate_cohort_df("B", uniform_distribution()),
        "C": generate_cohort_df("C", skewed_distribution(5)),
        "D": generate_cohort_df("D", skewed_distribution(-5)),
        "E": generate_cohort_df("E", skewed_distribution(0)),
        "F": generate_cohort_df("F", uniform_distribution())
    }

    sample_df = get_output_sample_df(generated_cohort_dfs)

    return pl.concat(
        [
            generated_cohort_dfs, sample_df
        ]
    )


def generate_sample_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "patent_num": [str(sample_patent) for sample_patent in SAMPLE],
            "issue_date": [sample_patent.issue_date for sample_patent in SAMPLE]
        }
    )


def generate_ipcr_df() -> pl.DataFrame:
    patent_id = [
        f"{prefix}{suffix}"
        for prefix in COHORT_YEARS.keys()
        for suffix in range(1, GENERATED_PATENTS_PER_COHORT + 1)
    ]
    section = [
        prefix
        for prefix in COHORT_YEARS.keys()
        for _ in range(GENERATED_PATENTS_PER_COHORT)
    ]

    generated_df = pl.DataFrame(
        {
            "patent_id": patent_id,
            "section": section,
        }
    )

    sample_df = pl.DataFrame(
        {
            "patent_id": [
                str(sample_patent)
                for sample_patent in SAMPLE
                for _ in range(len(sample_patent.citation_percentiles))
            ],
            "section": [
                section
                for sample_patent in SAMPLE
                for section in sample_patent.citation_percentiles.keys()
            ]
        }
    )
    return (
        pl.concat([generated_df, sample_df])
        .with_column(pl.lit(1).alias("ipc_class"))
        .with_column(pl.col("section").alias("subclass"))
        .with_row_count(name="uuid")
    )


def main() -> None:
    df = generate_ipcr_df()
    print(df)


if __name__ == '__main__':
    main()
