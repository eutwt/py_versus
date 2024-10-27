from __future__ import annotations

from typing import Dict, List, Iterable

import polars as pl
from polars.type_aliases import IntoExpr

from data_structures import Comparison, AorB

i_type = str | pl.DataType | Iterable[str] | Iterable[pl.DataType]
j_type = IntoExpr


def height(df: pl.DataFrame | pl.LazyFrame) -> int:
  return df.lazy().select(pl.len()).collect().item()


def subset(
  df: pl.LazyFrame,
  i: i_type,
  j: j_type = pl.all(),
) -> pl.LazyFrame:
  return df.select(j.gather(i))


def esubset(
  df: pl.DataFrame,
  i: i_type,
  j: j_type = pl.all(),
) -> pl.DataFrame:
  return df.select(j.gather(i))


def collect_first(df: pl.LazyFrame) -> pl.Series:
  out = (
    df.select(pl.selectors.first())
    .collect()
    .pipe(lambda x: x.get_column(x.columns[0]))
  )
  return out


def get_series(df: pl.LazyFrame, i: i_type, j: j_type) -> pl.Series:
  return collect_first(subset(df, i, j))


def init_df(comparison: Comparison, table: AorB) -> pl.DataFrame:
  out = getattr(comparison, table).filter(False).collect()
  return out


def contents(table: pl.DataFrame) -> pl.DataFrame:
  out_dict = {col: table[col].dtype for col in table.columns}

  out = pl.DataFrame(
    {"column": list(out_dict.keys()), "class": list(out_dict.values())}
  )
  return out


def get_cols_from_comparison(
  comparison: Comparison, column: List[str], allow_empty: bool = False
) -> Dict[str, int]:
  if not allow_empty and len(column) == 0:
    raise Exception("`column` argument must not be empty")
  comparison_cols = comparison["intersection"]["column"]
  if any(col not in comparison_cols for col in column):
    bad_col = next(col for col in column if col not in comparison_cols)
    raise Exception(
      f"Supplied column [{bad_col}] is not part of the comparison"
    )
  out_df = (
    comparison["intersection"]
    .lazy()
    .with_row_index(name="row")
    .filter(pl.col("column").is_in(column))
    .select("column", "row")
    .collect()
  )

  out = dict(zip(out_df["column"], out_df["row"]))
  return out


def identify_diff_cols(
  comparison: Comparison, column: List[str]
) -> Dict[str, int]:
  out_df = (
    comparison["intersection"]
    .lazy()
    .with_row_index(name="row")
    .filter(pl.col("column").is_in(column), pl.col("n_diffs") > 0)
    .select("column", "row")
    .collect()
  )

  out = dict(zip(out_df["column"], out_df["row"]))
  return out


def assert_is_single_column(column_loc: Dict[str, int]) -> None:
  if len(column_loc) > 1:
    raise Exception("Must select only one column")
