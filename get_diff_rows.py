from __future__ import annotations

import polars as pl

import helpers as h
from data_structures import Match


class IndexDF:
  def __init__(self, df: pl.DataFrame):
    self.df = df

  def __repr__(self):
    repr_str = f"IndexDF: ({self.df.height})"
    return repr_str

def get_diff_rows(
  col: str, table_a: pl.LazyFrame, table_b: pl.LazyFrame, matches: Match
) -> IndexDF:
  col_a = h.subset(table_a, matches["common"]["a"], pl.col(col).alias('a'))
  col_b = h.subset(table_b, matches["common"]["b"], pl.col(col).alias('b'))
  idx = (
    pl.concat([col_a, col_b], how="horizontal")
    .with_row_index('idx')
    .filter(pl.col('a') != pl.col('b'))
    .select('idx')
    .collect()
    ['idx']
  )
  out_df = h.subset(matches["common"].lazy(), idx).collect()
  out_df.columns = ["row_a", "row_b"]
  return IndexDF(out_df)
