from __future__ import annotations
from typing import List, Union

import polars as pl

import helpers as h
from data_structures import Comparison


def slice_diffs(
  comparison: Comparison, table: h.AorB, column: Union[str, List[str]]
) -> pl.DataFrame:
  if isinstance(column, str):
    column = [column]
  out = slice_diffs_impl(comparison, table, column, j = pl.all())
  return out

def slice_diffs_impl(
  comparison: Comparison,
  table: h.AorB,
  column: List[str],
  j: pl.IntoExpr | pl.Iterable[pl.IntoExpr], 
) -> pl.DataFrame:
  diff_cols = list(h.identify_diff_cols(comparison, column).values())
  if len(diff_cols) == 0:
    out = h.init_df(comparison, table).select(j)
    return out
  indexDFs = comparison["intersection"]["diff_rows"][diff_cols]
  rows = pl.concat(
    idxdf.df.select("row_" + table) for idxdf in indexDFs
  ).unique()["row_" + table]
  out = getattr(comparison, table).select(j).collect()[rows]
  return out
