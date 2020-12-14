import json
import urllib.request
from functools import reduce
from pyspark.sql.types import *

import pandas as pd

from utils.data_store.abstract_data_store import AbstractDataStore


class HBaseDataStore(AbstractDataStore):

    def __init__(self, spark):
        self.spark = spark
        return

    def read_pandas_df_from_data_store(self, **args):
        """
        Read table into PANDAS dataframe

        TODO complete docstring

        """
        return

    def read_spark_df_from_data_store(self, **args):
        """
        Read table into SPARK dataframe

        TODO complete docstring

        """
        url = args["hbase_url"]
        numerical_colnames = args["numerical_colnames"]
        actor_type = args["actor_type"]
        colname_to_content_dict = dict()
        categorical_colnames = args["categorical_colnames"]
        logger = args["logger"]

        for numerical_colname in numerical_colnames:
            content_url = url.replace("$attribute_name$", numerical_colname)
            logger.info(content_url)
            contents = urllib.request.urlopen(content_url).read()
            data = json.loads(contents)
            df = pd.DataFrame(list(data.items()), columns=[actor_type, numerical_colname])
            colname_to_content_dict[numerical_colname] = df

        for categorical_colname in categorical_colnames:
            content_url = url.replace("$attribute_name$", categorical_colname)
            logger.info(content_url)
            contents = urllib.request.urlopen(content_url).read()
            data = json.loads(contents)
            df = pd.DataFrame(list(data.items()), columns=[actor_type, categorical_colname])
            colname_to_content_dict[categorical_colname] = df

        dfs = colname_to_content_dict.values()
        df = reduce(lambda left, right: pd.merge(left, right, on=actor_type), dfs)
        struct_field_list = list()
        struct_field_list.append(StructField(args["actor_type"], StringType(), True))
        for numerical_colname in numerical_colnames:
            struct_field_list.append(StructField(numerical_colname, FloatType(), True))
        for categorical_colname in categorical_colnames:
            struct_field_list.append(StructField(categorical_colname, StringType(), True))
        schema = StructType(struct_field_list)
        sdf = self.spark.createDataFrame(df, schema)
        return sdf

