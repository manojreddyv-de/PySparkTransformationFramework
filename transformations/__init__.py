from .basic_ops import select_cols, filter_rows, add_column, drop_cols
from .custom_ops import custom_logic

TRANSFORMATIONS = {
    "select": select_cols,
    "filter": filter_rows,
    "withColumn": add_column,
    "drop": drop_cols,
    "custom": custom_logic,
}
