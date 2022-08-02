import polars as pl

from importlib import resources

DATA_PATH = resources.files("patent_analysis.data")


def main():
    citations = pl.scan_csv(f"{DATA_PATH}/citations/output_sample.tsv", sep="\t")
    citations_dummy = pl.scan_csv(f"{DATA_PATH}/citations_dummy/output.tsv", sep="\t")

    lf = citations.join(
        citations_dummy,
        left_on="cited_patent",
        right_on="cited_patent"
    )

    lf.collect().write_csv(f"{DATA_PATH}/joined.tsv", sep="\t")


if __name__ == '__main__':
    main()
