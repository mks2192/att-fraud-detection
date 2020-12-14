from utils.data_store.abstract_data_store import AbstractDataStore


class HiveDataStore(AbstractDataStore):
    def __init__(self, spark):
        """
        TODO complete docstring
        :param src_dir:
        """
        self.spark = spark
        # ensure path ends with a forward slash

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
        colnames = args["colnames"]
        database = args["tenant_name"]
        table = args["data_source"]
        colnames_string = ",".join(colnames)
        query = "select " + colnames_string + " from " + database + "." + table + "_stream where ip_src_addr!='NULL' and ip_dst_addr!='NULL' and y>=2019 and m>=5 and d>=23"
        print(query)
        sdf = self.spark.sql(query).persist()
        return sdf

    def read_spark_df(self, **args):
        database = args["database"]
        table = args["table"]
        sdf = self.spark.sql(
            "select * from " + database + "." + table).persist()
        return sdf
