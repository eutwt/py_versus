
import polars as pl
from typing import List, Dict

def contents(table: pl.DataFrame) -> pl.DataFrame:
    out_dict = {col: str(table[col].dtype) for col in table.columns}
    
    out = pl.DataFrame({
        "column": list(out_dict.keys()),
        "class": list(out_dict.values())
    })
    return out

def get_cols_from_comparison(
    comparison: List[pl.DataFrame],
    column: List[str],
    allow_empty: bool = False) -> Dict[str, int]:

  if not allow_empty and len(column) == 0:
    raise Exception("`column` argument must not be empty")
  comparison_cols = comparison['intersection']['column']
  if any(col not in comparison_cols for col in column):
    bad_col = next(col for col in column if col not in comparison_cols)
    raise Exception(f"Supplied column [{bad_col}] is not part of the comparison")
  out_df = comparison['intersection'] \
    .lazy() \
    .with_row_index(name = 'row') \
    .filter(pl.col('n_diffs') > 0, pl.col('column').is_in(column)) \
    .select('column', 'row') \
    .collect()
  
  out = dict(zip(out_df['column'], out_df['row']))
  return out

def assert_is_single_column(column_loc: Dict[str, int]) -> None:
  if len(column_loc) > 1:
    raise Exception("Must select only one column")  
