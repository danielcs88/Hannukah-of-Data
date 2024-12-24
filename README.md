# Hannukah of Data

These are my solutions in [Pandas](https://pandas.pydata.org/) for [Hannukah of
Data](https://hanukkah.bluebird.sh).

For most of my answers, I attempted to be as single execution as possible.
Leveraging
[`pd.DataFrame.pipe`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.pipe.html)
as much as possible to resemble R and SQL.

Thanks to [saulpw](https://www.saul.pw/) and the
[Devottys](https://github.com/devottys) for these amazing puzzles.

For 2024 [(Noah's Rug)](https://www.whereinthedata.com/noahsrug/), I decided to
give [Polars](https://pola.rs/) a shot.

## Requirements

Other than Pandas, the sole requirement for this is
[Pyperclip](https://pypi.org/project/pyperclip/), highly recommended to
manipulate the clipboard with Python.

For any curious why there is an identical `.py` and a `.ipynb` file, I usually
write everything like a Jupyter Notebook as a script by using
[Jupytext](https://jupytext.readthedocs.io/en/latest/install.html). While this
is not a requirement to run either; it is a nicety to use both advantages of a
Python script and the exploratory nature of Jupyter Notebooks.
