from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Union

import polars as pl

import get_diff_rows as diff
import helpers as h

if TYPE_CHECKING:
  from data_structures import Comparison, Match, RawComparison


def compare(
  table_a: pl.DataFrame, table_b: pl.DataFrame, by: Union[str, List[str]]
) -> Comparison:
  if isinstance(by, str):
    by = [by]
  lazy_a = table_a.lazy()
  lazy_b = table_b.lazy()
  table_summ = pl.DataFrame(
    {
      "table": ["table_a", "table_b"],
      "height": [table_a.height, table_b.height],
      "width": [table_a.width, table_b.width],
    }
  )
  tbl_contents = get_contents(table_a, table_b, by=by)
  matches = locate_matches(lazy_a, lazy_b, by=by)
  unmatched_rows = get_unmatched_rows(table_a, table_b, by=by, matches=matches)

  diff_rows = [
    diff.get_diff_rows(col, lazy_a, lazy_b, matches=matches)
    for col in tbl_contents["compare"]["column"]
  ]
  n_diffs = pl.Series(x.df.height for x in diff_rows)
  tbl_contents["compare"] = (
    tbl_contents["compare"]
    .with_columns(
      diff_rows=pl.Series(diff_rows, dtype=pl.Object), n_diffs=n_diffs
    )
    .select("column", "n_diffs", "class_a", "class_b", "diff_rows")
  )

  comparison = {
    "tables": table_summ,
    "by": tbl_contents["by"],
    "intersection": tbl_contents["compare"],
    "unmatched_cols": tbl_contents["unmatched_cols"],
    "unmatched_rows": unmatched_rows,
  }
  tables = {"a": lazy_a, "b": lazy_b}
  out: RawComparison = {"comparison": comparison, "tables": tables}
  # todo: better fix for circular dependency
  from data_structures import Comparison

  return Comparison(out)


def vec_locate_matches(
  table_a: pl.LazyFrame, table_b: pl.LazyFrame, by: List[str]
) -> pl.DataFrame:
  a_with_ind = table_a.with_row_index(name="vs_a_index")
  b_with_ind = table_b.with_row_index(name="vs_b_index")

  merged = b_with_ind.join(
    a_with_ind, on=by, how="full", join_nulls=True, validate="1:1"
  )
  out = merged.select(a=pl.col("vs_a_index"), b=pl.col("vs_b_index"))

  return out.collect()


def split_matches(matches: pl.DataFrame) -> Match:
  """
  split matches into
    common: rows in both tables
    a: rows only in table_a
    b: rows only in table_b
  """
  is_a_only = matches["b"].is_null()
  is_b_only = matches["a"].is_null()
  out: Match = {
    "common": matches.filter(~is_a_only & ~is_b_only),
    "a": matches["a"].filter(is_a_only),
    "b": matches["b"].filter(is_b_only),
  }

  return out


def locate_matches(
  table_a: pl.LazyFrame, table_b: pl.LazyFrame, by: List[str]
) -> Match:
  match_df = vec_locate_matches(table_a, table_b, by)
  out = split_matches(match_df)
  return out


def get_unmatched_rows(
  table_a: pl.DataFrame, table_b: pl.DataFrame, by: List[str], matches: Match
) -> pl.DataFrame:
  def get_unmatched(name, df):
    out = df.pipe(h.esubset, matches[name], pl.col(by)).select(
      [pl.lit(name).alias("table")] + by
    )
    return out

  tables = {"a": table_a, "b": table_b}.items()
  out = pl.concat(get_unmatched(name, df) for name, df in tables).with_columns(
    row=pl.concat([matches["a"], matches["b"]])
  )

  return out


def converge(
  table_a: pl.DataFrame, table_b: pl.DataFrame, by: List[str], matches: Match
) -> pl.DataFrame:
  common_cols = [
    col
    for col in table_a.columns
    if (col in table_b.columns) and (col not in by)
  ]

  by_a = table_a.pipe(h.esubset, matches["common"]["a"], pl.col(by))
  common_a = table_a.pipe(h.esubset, matches["common"]["a"], pl.col(common_cols))
  common_b = table_b.pipe(h.esubset, matches["common"]["b"], pl.col(common_cols))
  common_a.columns = [col + "_a" for col in common_cols]
  common_b.columns = [col + "_b" for col in common_cols]

  out = pl.concat([by_a, common_a, common_b], how="horizontal")
  return out


def join_split(
  table_a: pl.DataFrame, table_b: pl.DataFrame, by: List[str]
) -> Dict[str, pl.DataFrame]:
  matches = locate_matches(table_a.lazy(), table_b.lazy(), by)
  intersection = converge(table_a, table_b, by, matches)
  unmatched_rows = get_unmatched_rows(table_a, table_b, by, matches)
  out = {"intersection": intersection, "unmatched_rows": unmatched_rows}
  return out


def get_contents(
  table_a: pl.DataFrame, table_b: pl.DataFrame, by: List[str]
) -> Dict[str, pl.DataFrame]:
  tbl_contents = join_split(
    h.contents(table_a), h.contents(table_b), by=["column"]
  )
  out = {}
  out["by"] = tbl_contents["intersection"].filter(pl.col("column").is_in(by))
  out["compare"] = tbl_contents["intersection"].filter(
    ~pl.col("column").is_in(by)
  )
  out["unmatched_cols"] = tbl_contents["unmatched_rows"].drop("row")
  return out
