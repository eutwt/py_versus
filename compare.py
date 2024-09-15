import helpers as h
import get_diff_rows as diff
from typing import Dict, Union, List 
import polars as pl

Match = Dict[str, Union[pl.DataFrame, pl.Series]] #todo: remove dupe def

def compare(
    table_a: pl.DataFrame, table_b: pl.DataFrame, by: Union[str, List[str]]) -> List[pl.DataFrame]:
  
  if isinstance(by, str):
    by = [by]
  
  table_summ = pl.DataFrame({
    'table': ['table_a', 'table_b'],
    'nrow': [table_a.height, table_b.height],
    'ncol': [table_a.width, table_b.width]
  })
  tbl_contents = get_contents(table_a, table_b, by = by)
  matches = locate_matches(table_a, table_b, by = by)
  unmatched_rows = get_unmatched_rows(table_a, table_b, by = by, matches = matches)
  
  diff_rows = [
    diff.get_diff_rows(col, table_a, table_b, matches = matches)
    for col in tbl_contents['compare']['column']]
  n_diffs = [len(x['row_a']) for x in diff_rows]
  tbl_contents['compare'] = tbl_contents['compare'] \
    .with_columns(diff_rows = pl.Series(diff_rows),
                  n_diffs = pl.Series(n_diffs)) \
    .select('column', 'n_diffs', 'class_a', 'class_b', 'diff_rows')

  out = {
    'tables': table_summ,
    'by': tbl_contents['by'],
    'intersection': tbl_contents['compare'],
    'unmatched_cols': tbl_contents['unmatched_cols'],
    'unmatched_rows': unmatched_rows
  }

  return out

def vec_locate_matches(
    table_a: pl.LazyFrame, table_b: pl.LazyFrame, by: List[str]) -> pl.DataFrame:

  lazy_a = table_a.lazy().with_row_index(name='vs_a_index')
  lazy_b = table_b.lazy().with_row_index(name='vs_b_index')
  
  merged = lazy_b.join(lazy_a, on=by, how='full', join_nulls=True, validate='1:1')
  out = merged.select(a = pl.col('vs_a_index'), b = pl.col('vs_b_index'))
  
  return out.collect()


def split_matches(matches: pl.DataFrame) -> Match:
  '''
  split matches into
    common: rows in both tables
    a: rows only in table_a
    b: rows only in table_b    
  '''
  is_b_only = matches['a'].is_null()
  is_a_only = matches['b'].is_null()
  out = {
    'common': matches.filter(~is_b_only & ~is_a_only),
    'a': matches['a'].filter(is_a_only), 
    'b': matches['b'].filter(is_b_only)}
  
  return out
  
def locate_matches(
    table_a: pl.LazyFrame, table_b: pl.LazyFrame, by: List[str]) -> Match:
    
  match_df = vec_locate_matches(table_a, table_b, by)
  out = split_matches(match_df)
  return out

def get_unmatched_rows(
    table_a: pl.DataFrame,
    table_b: pl.DataFrame,
    by: List[str],
    matches: Match) -> pl.DataFrame:
  
  def get_unmatched(name, df):
    out = df.select(by)[matches[name]] \
      .select([pl.lit(name).alias('table')] + by)
    return out
  
  unmatched = pl.concat(
    get_unmatched(name, df)
    for name, df in {'a': table_a, 'b': table_b}.items())
    
  out = unmatched \
    .with_columns(row = pl.concat([matches['a'], matches['b']]))
    
  return out

def converge(
    table_a: pl.DataFrame,
    table_b: pl.DataFrame,
    by: List[str],
    matches: Match) -> pl.DataFrame:
      
  common_cols = [
    col for col in table_a.columns
    if (col in table_b.columns) and (col not in by)]
    
  by_a = table_a.select(by)[matches['common']['a']]
  common_a = table_a.select(common_cols)[matches['common']['a']]
  common_b = table_b.select(common_cols)[matches['common']['b']]
  common_a.columns = [col + '_a' for col in common_cols]
  common_b.columns = [col + '_b' for col in common_cols]
    
  out = pl.concat([by_a, common_a, common_b], how = 'horizontal')
  return out      

def join_split(
    table_a: pl.DataFrame, table_b: pl.DataFrame, by: str) -> List[pl.DataFrame]:
  matches = locate_matches(table_a, table_b, by)
  intersection = converge(table_a, table_b, by, matches)
  unmatched_rows = get_unmatched_rows(table_a, table_b, by, matches)
  out = {'intersection': intersection, 'unmatched_rows': unmatched_rows}
  return out

def get_contents(
    table_a: pl.DataFrame, table_b: pl.DataFrame, by: str) -> List[pl.DataFrame]:
  
  tbl_contents = join_split(h.contents(table_a), h.contents(table_b), by = ['column'])
  out = {}
  out['by'] = tbl_contents['intersection'] \
    .filter(pl.col('column').is_in(by))
  out['compare'] = tbl_contents['intersection'] \
    .filter(~pl.col('column').is_in(by))
  out['unmatched_cols'] = tbl_contents['unmatched_rows'].drop('row')
  return out

