# Patent data analysis
This repo contains code that counts patent citations.

## Data source
The data used by this repository is provided by PatentsView.

## `resources` packages layout
* `patentsview` is for the original files from [patentsview.org](https://patentsview.org/download/data-download-tables).
    These files are too large, so they are not committed.
* `truncated` contains the tables limited to 10 lines, used to test reading in the data.
* `mini` contains hand-made data, used to test data operations.
* `stripped` contains every row from `patentsview`, but without quotes and with only the relevant columns.
    These files are too large, so they are not committed.

