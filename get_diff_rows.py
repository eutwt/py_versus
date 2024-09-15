
import polars as pl
from typing import Dict, Union

Match = Dict[str, Union[pl.DataFrame, pl.Series]]

def get_diff_rows(
    col: str,
    table_a: pl.DataFrame,
    table_b: pl.DataFrame,
    matches: Match) -> Dict[str, pl.Series]:

  col_a = table_a[col][matches['common']['a']]
  col_b = table_b[col][matches['common']['b']]
  out_df = matches['common'].filter(col_a != col_b)
  print('hi', flush = True)
  print(out_df['a'])
  print(type(out_df['a']))
  out = {
    'row_a': out_df['a'],
    'row_b': out_df['b']
  }
  
  return out