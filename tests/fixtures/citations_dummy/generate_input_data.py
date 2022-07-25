"""
This file generates all the fixtures
for citations_dummy besides citations_dummy/output.tsv.
"""

import pandas as pd
import polars as pl
import numpy as np
from scipy.stats import skewnorm

from datetime import datetime

SAMPLE_DATES = {
    "A": datetime(1999, 3, 22),
    "B": datetime(2000, 4, 7),
    "C": datetime(2017, 11, 17),
    "D": datetime(2016, 3, 29),
    "E": datetime(2015, 10, 29)
}

GENERATED_PATENTS_PER_COHORT = 99

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

    year = SAMPLE_DATES[prefix].year
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
    # TODO
    return None


def generate_sample_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "patent_num": SAMPLE_DATES.keys(),
            "issue_date": SAMPLE_DATES.values()
        }
    )


def generate_ipcr_df() -> pl.DataFrame:
    uuid = range(1, 5 * (GENERATED_PATENTS_PER_COHORT + 1) + 1)
    patent_id = [
        f"{prefix}{suffix}"
        for prefix in SAMPLE_DATES.keys()
        for suffix in ["", *range(1, GENERATED_PATENTS_PER_COHORT + 1)]
    ]
    section = [
        prefix
        for prefix in SAMPLE_DATES.keys()
        for _ in range(1 + GENERATED_PATENTS_PER_COHORT)
    ]
    ipc_class = pl.lit(1)
    subclass = section

    return pl.DataFrame(
        {
            "uuid": uuid,
            "patent_id": patent_id,
            "section": section,
            "ipc_class": ipc_class,
            "subclass": subclass
        }
    )


def main() -> None:
    generate_output_universe_df()


if __name__ == '__main__':
    main()
