import traceback
import uuid
from io import BytesIO as BytesIO
from typing import Dict, List, Tuple, Union, Any

try:
    import pandas as _pd  # noqa
    import pyarrow as _pa
    import pyarrow.parquet as _pq
except ImportError:
    pass

from .constant import (
    COMPRESSION_EXTENSION as _COMPRESSION_EXTENSION,
)


def generate_table(
    data: List[Dict[str, Any]],
    from_date: str,
    rename_columns: Union[Dict[str, str], None] = None,
) -> Union["_pa.Table", None]:
    try:
        if not isinstance(data, list):
            raise TypeError(f"Expected list, {type(data).__name__} found")

        if not data:
            return None

        rename_columns = rename_columns or {}

        df = _pd.json_normalize(data)
        df["ee_recorded_at"] = from_date
        table = _pa.Table.from_pandas(df)

        return table

    except Exception as e:
        print("Error when generating PyArrow Table: ", traceback.format_exc())
        raise e


def _rename_columns(
    table: "_pa.Table", new_column_names: Dict[str, str]
) -> "_pa.Table":

    try:
        if table is None:
            raise TypeError("expected pyarrow.Table, NoneType found")

        if not new_column_names:
            return table

        new_names = [new_column_names.get(name, name) for name in table.column_names]
        new_table = table.rename_columns(new_names)
        new_table.validate()

        return new_table

    except Exception as e:
        print("Error when rename columns: ", traceback.format_exc())
        raise e


def write_parquet(
    table: "_pa.Table",
    path_root: str,
    compression: str = "snappy",
    pyarrow_additional_kwargs: Union[Dict[str, str], None] = None,
) -> Tuple[BytesIO, str]:
    

    try:
        pyarrow_additional_kwargs = pyarrow_additional_kwargs or {}
        compression_ext = _COMPRESSION_EXTENSION.get(compression)
        obj_key = path_root + f"{uuid.uuid4().hex}{compression_ext}.parquet"

        with _pa.BufferOutputStream() as out:
            _pq.write_table(
                table, where=out, compression=compression, **pyarrow_additional_kwargs
            )
            file = BytesIO(out.getvalue().to_pybytes())

        return file, obj_key

    except Exception as e:
        print("Error when writing to Parquet: ", traceback.format_exc())
        raise e
