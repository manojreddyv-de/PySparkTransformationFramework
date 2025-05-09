import json
from pyspark.sql import SparkSession
from transformations import TRANSFORMATIONS
from actions import ACTIONS
from utils.logger import get_logger
logger = get_logger(__name__)

def apply_pipeline(df, config):
    for step in config["transformations"]:
        func = TRANSFORMATIONS[step["operation"]]
        df = func(df, step["params"])

    for action in config["actions"]:
        func = ACTIONS[action["type"]]
        func(df, action["params"])
    
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DynamicTransformer").getOrCreate()
    
    # Replace this with actual input
    df = spark.read.json("data/input.json")

    # Example config from JSON
    with open("config/pipeline_config.json") as f:
        config = json.load(f)

    apply_pipeline(df, config)
