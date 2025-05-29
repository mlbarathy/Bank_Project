from pyspark.sql.types import *

def spark_to_postgres_type(data_type):
    if isinstance(data_type, StringType):
        return "TEXT"
    elif isinstance(data_type, IntegerType):
        return "INTEGER"
    elif isinstance(data_type, LongType):
        return "BIGINT"
    elif isinstance(data_type, DoubleType):
        return "DOUBLE PRECISION"
    elif isinstance(data_type, BooleanType):
        return "BOOLEAN"
    elif isinstance(data_type, TimestampType):
        return "TIMESTAMP"
    return "TEXT"

def spark_to_postgres_type(py_type):
    if py_type == int:
        return "INTEGER"
    elif py_type == float:
        return "DOUBLE PRECISION"
    elif py_type == bool:
        return "BOOLEAN"
    else:
        return "TEXT"
