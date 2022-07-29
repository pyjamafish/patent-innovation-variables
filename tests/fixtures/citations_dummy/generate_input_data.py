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
class SamplePatent:
    issue_date: datetime
    citation_counts: tuple[int, Optional[int]]
    subclasses: list[str]

    def __str__(self):
        return f"{''.join(self.subclasses)}/{self.citation_counts[0]}-{self.citation_counts[1]}"


SAMPLE = [
    SamplePatent(
        datetime(1999, 3, 22),
        (938, 1124),
        ["A", "B"]
    ),
    SamplePatent(
        datetime(1999, 4, 7),
        (942, 942),
        ["A, B"]
    ),
    SamplePatent(
        datetime(2017, 11, 17),
        (1001, None),
        ["C", "D"]
    ),
    SamplePatent(
        datetime(2017, 3, 29),
        (790, None),
        ["C", "D"]
    ),
    SamplePatent(
        datetime(2000, 10, 29),
        (1001, 1201),
        ["E", "F"]
    ),
    SamplePatent(
        datetime(2000, 10, 25),
        (420, 1064),
        ["E", "F"]
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
    ).astype(int)


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


def generate_output_universe_df() -> pl.DataFrame:
    generated_cohort_dfs = [
        generate_cohort_df("A", skewed_distribution(-10)),
        generate_cohort_df("B", uniform_distribution()),
        generate_cohort_df("C", skewed_distribution(5)),
        generate_cohort_df("D", skewed_distribution(-5)),
        generate_cohort_df("E", skewed_distribution(0)),
        generate_cohort_df("F", uniform_distribution())
    ]

    sample_df = (
        pl.DataFrame(
            {
                "cited_patent": [str(sample_patent) for sample_patent in SAMPLE],
                "cited_patent_issue_date": [sample_patent.issue_date for sample_patent in SAMPLE],
                "citations_3_years": [sample_patent.citation_counts[0] for sample_patent in SAMPLE],
                "citations_5_years": [sample_patent.citation_counts[1] for sample_patent in SAMPLE]
            }
        )
        .with_column(pl.col("cited_patent_issue_date").cast(pl.Date))
        .with_column(pl.col("citations_3_years").cast(pl.Int64))
        .with_column(pl.col("citations_5_years").cast(pl.Int64))
    )

    pass

    return pl.concat(
        [
            *generated_cohort_dfs, sample_df
        ]
    )


def generate_sample_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "patent_num": [str(sample_patent) for sample_patent in SAMPLE],
            "issue_date": [sample_patent.issue_date.strftime("%m/%d/%Y") for sample_patent in SAMPLE],
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
                for _ in range(len(sample_patent.subclasses))
            ],
            "section": [
                section
                for sample_patent in SAMPLE
                for section in sample_patent.subclasses
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
    output_universe_df = generate_output_universe_df()
    output_universe_df.write_csv("citations/output_universe.tsv", sep="\t")

    sample_df = generate_sample_df()
    sample_df.write_csv("citations/sample.csv")

    ipcr_df = generate_ipcr_df()
    ipcr_df.write_csv("citations_dummy/ipcr.tsv", sep="\t")


if __name__ == '__main__':
    main()
