
import polars as pl

def contents(table: pl.DataFrame) -> pl.DataFrame:
    out_dict = {col: str(table[col].dtype) for col in table.columns}
    
    out = pl.DataFrame({
        "column": list(out_dict.keys()),
        "class": list(out_dict.values())
    })
    return out
