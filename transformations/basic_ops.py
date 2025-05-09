from utils.logger import get_logger

logger = get_logger(__name__)

def select_cols(df, params):
    logger.info(f"Selecting columns: {params['columns']}")
    return df.select(*params["columns"])

def filter_rows(df, params):
    logger.info(f"Applying filter condition: {params['condition']}")
    return df.filter(params["condition"])

def add_column(df, params):
    from pyspark.sql.functions import expr
    logger.info(f"Adding column '{params['name']}' with expression: {params['expression']}")
    return df.withColumn(params["name"], expr(params["expression"]))

def drop_cols(df, params):
    logger.info(f"Dropping columns: {params['columns']}")
    return df.drop(*params["columns"])
