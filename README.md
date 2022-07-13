# Patent data analysis
## Data Source
The data used by this repository is provided by PatentsView.

## `resources` Layout
* `truncated` contains the tables limited to 10 lines, to test reading in the data.
* `mini` contains hand-made test tables, to test data operations.
* `patentsview` is for the original files from [patentsview.org](https://patentsview.org/download/data-download-tables).
    These files are too large, so they are not committed.
* `stripped` contains every row from `patentsview`, but without quotes and with only the relevant columns.
    These files are too large, so they are not committed.

