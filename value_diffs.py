from __future__ import annotations
from typing import List

import polars as pl

import helpers as h
from data_structures import Comparison


def value_diffs(comparison: Comparison, column: str) -> pl.DataFrame:
  column_loc = h.get_cols_from_comparison(comparison, [column])
  h.assert_is_single_column(column_loc)
  if len(column_loc) == 0:
    a = h.init_df(comparison, "a").select(pl.col(column).alias(column + "_a"))
    b = h.init_df(comparison, "b").select(pl.col(column).alias(column + "_b"))
    by = h.init_df(comparison, "a").select(list(comparison["by"]["column"]))
    out = pl.concat([a, b, by], how="horizontal")
    return out

  diff_rows = comparison["intersection"].item(
    list(column_loc.values())[0], "diff_rows"
  )
  col = list(column_loc.keys())[0]
  a = comparison.a.select(pl.col(col).alias(col + "_a")).collect()[
    diff_rows.df["row_a"]
  ]
  b = comparison.b.select(pl.col(col).alias(col + "_b")).collect()[
    diff_rows.df["row_b"]
  ]
  by = comparison.a.select(list(comparison["by"]["column"])).collect()[
    diff_rows.df["row_a"]
  ]
  out = pl.concat([a, b, by], how="horizontal")
  return out


def value_diffs_stacked(
  comparison: Comparison, column: List[str]
) -> pl.DataFrame:
  def get_value_diff_for_stack(
    comparison: Comparison, col_name: str
  ) -> pl.DataFrame:
    out = (
      comparison.value_diffs(col_name)
      .with_columns(column=pl.lit(col_name))
      .rename({col_name + "_a": "val_a", col_name + "_b": "val_b"})
    )
    out = out.select(["column"] + [nm for nm in out.columns if nm != "column"])
    return out

  diff_cols = h.identify_diff_cols(comparison, column)
  if len(diff_cols) == 0:
    diff_cols = h.get_cols_from_comparison(comparison, column)

  diffs = [get_value_diff_for_stack(comparison, col) for col in diff_cols]
  out = pl.concat(diffs, how="vertical_relaxed")
  return out
