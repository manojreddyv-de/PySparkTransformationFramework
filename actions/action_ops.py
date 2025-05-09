from utils.logger import get_logger
logger = get_logger(__name__)

def show_df(df, params):
    return df.show(params.get("numRows", 5), truncate=False)

def collect_df(df, params):
    return df.collect()

def write_df(df, params):
    return df.write.mode(params["mode"]).format(params["format"]).save(params["path"])
