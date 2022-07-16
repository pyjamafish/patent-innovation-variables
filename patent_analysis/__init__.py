from datetime import datetime
from dateutil.relativedelta import relativedelta

DATA_LATEST_DATE = datetime(2021, 12, 31)

SAMPLE_EARLIEST_DATE = datetime(1999, 1, 1)
SAMPLE_LATEST_DATE = DATA_LATEST_DATE - relativedelta(years=3)

SAMPLE_5_YEAR_CUTOFF = DATA_LATEST_DATE - relativedelta(years=5)
