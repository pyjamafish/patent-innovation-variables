"""
This file generates all the fixtures
for citations_dummy besides citations_dummy/output.tsv.
"""

import pandas as pd
import polars as pl
import numpy as np
from scipy.stats import skewnorm

from datetime import datetime

COHORT_YEARS = {
    "A": 1999,
    "B": 1999,
    "C": 2017,
    "D": 2017,
    "E": 2000,
    "F": 2000
}

SAMPLE = {
    "A-60-60/B-94-94": datetime(1999, 3, 22),
    "A-95-94/B-20-20": datetime(1999, 4, 7),
    "C-99-null/D-95-null": datetime(2017, 11, 17),
    "C-94-null/D-95-null": datetime(2017, 3, 29),
    "E-32-32/F-99-99": datetime(2000, 10, 29),
    "E-94-98/F-60-60": datetime(2000, 10, 25)
}

GENERATED_PATENTS_PER_COHORT = 98

SEED = 521


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
    )


def generate_output_universe_df() -> pl.DataFrame:
    a = generate_cohort_df("A", skewed_distribution(-10))
    b = generate_cohort_df("B", uniform_distribution())
    c = generate_cohort_df("C", skewed_distribution(5))
    d = generate_cohort_df("D", skewed_distribution(-5))
    e = generate_cohort_df("E", skewed_distribution(0))
    f = generate_cohort_df("F", uniform_distribution())
    # TODO
    return None


def generate_sample_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "patent_num": SAMPLE.keys(),
            "issue_date": SAMPLE.values()
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
                patent_id
                for patent_id in SAMPLE.keys()
                for _ in range(2)
            ],
            "section": [
                s
                for patent_id in SAMPLE.keys()
                for s in (patent_id[0], patent_id.partition("/")[2][0])
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
