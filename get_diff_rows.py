from __future__ import annotations

import polars as pl

from data_structures import Match


class IndexDF:
  def __init__(self, df: pl.DataFrame):
    self.df = df

  def __repr__(self):
    repr_str = f"IndexDF: ({self.df.height})"
    return repr_str


def get_diff_rows(
  col: str, table_a: pl.DataFrame, table_b: pl.DataFrame, matches: Match
) -> IndexDF:
  col_a = table_a[col][matches["common"]["a"]]
  col_b = table_b[col][matches["common"]["b"]]
  out_df = matches["common"].filter(col_a != col_b)
  out_df.columns = ["row_a", "row_b"]
  return IndexDF(out_df)
