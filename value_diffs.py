from __future__ import annotations

import polars as pl

import helpers as h
from data_structures import Comparison


def value_diffs(comparison: Comparison, column: str) -> pl.DataFrame:
  column_loc = h.get_cols_from_comparison(comparison, [column])
  h.assert_is_single_column(column_loc)
  if len(column_loc) == 0:
    by = (
      comparison["input"]["a"]
      .select(list(comparison["by"]["column"]))
      .filter(False)
      .collect()
    )
    a_b = pl.DataFrame({column + "_a": [], column + "_b": []})
    out = a_b.hstack(by)
    return out

  diff_rows = comparison["intersection"].item(
    list(column_loc.values())[0], "diff_rows"
  )
  col = list(column_loc.keys())[0]
  a = (
    comparison["input"]["a"]
    .select(pl.col(col).alias(col + "_a"))
    .collect()[diff_rows.df["row_a"]]
  )
  b = (
    comparison["input"]["b"]
    .select([pl.col(col).alias(col + "_b")] + list(comparison["by"]["column"]))
    .collect()[diff_rows.df["row_b"]]
  )

  out = a.hstack(b)

  return out
