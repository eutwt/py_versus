from __future__ import annotations

from collections import UserDict
from typing import Dict, List, TypedDict, Union, Literal

import polars as pl

AorB = Union[Literal["a"], Literal["b"]]

class Comparison(UserDict):
  def __init__(self, data: RawComparison):
    self.a = data["tables"]["a"]
    self.b = data["tables"]["b"]
    super().__init__(data["comparison"])

  def value_diffs(self, column: str) -> pl.DataFrame:
    # todo: better way to avoid circular import?
    import value_diffs as vd

    return vd.value_diffs(self, column=column)

  def value_diffs_stacked(self, column: List[str]) -> pl.DataFrame:
    # todo: better way to avoid circular import?
    import value_diffs as vd

    return vd.value_diffs_stacked(self, column=column)

  def slice_diffs(self, table: AorB, column: Union[str, List[str]]) -> pl.DataFrame:
    # todo: better way to avoid circular import?
    import slice_diffs as sd

    return sd.slice_diffs(self, table=table, column=column)

class Match(TypedDict):
  common: pl.DataFrame
  a: pl.Series
  b: pl.Series


class RawComparison(TypedDict):
  comparison: Dict[str, pl.DataFrame]
  tables: Dict[str, pl.LazyFrame]
