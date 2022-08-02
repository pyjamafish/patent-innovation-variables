# Patent Innovation Variables
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.6954668.svg)](https://doi.org/10.5281/zenodo.6954668)

This repo contains code that calculates variables that indicate patent innovation.

## Data sources
The patent and citation data used by this repository is provided by PatentsView.

The sample used by this repository is taken from KPSS.
> Leonid Kogan, Dimitris Papanikolaou, Amit Seru, Noah Stoffman, Technological Innovation, Resource Allocation, and Growth, The Quarterly Journal of Economics, Volume 132, Issue 2, May 2017, Pages 665–712, https://doi.org/10.1093/qje/qjw040

## `data` package layout
* `citation` subpackage: contains files for counting citations.
These files are too big to commit, so the expected files are listed below:
  * `output_sample.tsv`: the output data for only the patents in `sample.csv`.
  * `output_universe.tsv`: the full output data.
  * `patent.tsv`: the patent table from PatentsView,
  used to match patent ID with issue date.
  * `sample.csv`: the sample of patents to track citations for, from KPSS.
  * `uspatentcitation.tsv`: the citation table from PatentsView.
* `citations_dummy` subpackage: contains files for determining citation dummy variables.
  * `ipcr.tsv`: the table from PatentsView that matches patents with their section/class/subclass combination(s).
  * `output.tsv`: the ouput data.

