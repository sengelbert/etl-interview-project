from pyspark.sql import SparkSession
import yaml


class Sparky:
    """Class for reading configuration files and processing data via Spark

    """

    def __init__(self):
        """Class constructor, initializes SparkSession

        """
        print('calling __init__() constructor for Sparky...')
        self.sqlContext = None
        self.sparky = SparkSession.builder.appName("Spark App").getOrCreate()
        self.spark_context = self.sparky.sparkContext
        self.spark_context.setLogLevel("ERROR")

    def read_yaml(self, file):
        """Function reads a YAML config file into a dictionary.

        :param file: The path and filename for the YAML file
        :type file: str
        :returns: The config dictionary
        :rtype: dict
        """

        print(f"Reading {file}...")
        with open(file, 'r') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)

        return data

    def create_source_schema(self, file):
        """Function creates a Spark schema based on a YAML config file.

        :param file: The path and filename for the YAML file
        :type file: str
        :returns: Spark Schema
        :rtype: pyspark.sql.types.StructType
        """

        from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, IntegerType, \
            DoubleType, LongType

        columns = []
        definition = self.read_yaml(file)
        print(f"Collecting Source Columns and Data Types from {file}...")
        for column in definition['source']['file']['fields']:
            if column['type'] == 'string':
                data_type = StringType()
            elif column['type'] == 'int':
                data_type = IntegerType()
            elif column['type'] == 'date':
                data_type = DateType()
            # TODO some day add other data types

            columns.append(StructField(column['name'], data_type, True))
        # print(columns)

        return StructType(columns)

    def create_target_query(self, file):
        """Function creates a SparkSQL query based on logic from a YAML config file.

        :param file: The path and filename for the YAML file
        :type file: str
        :returns: SparkSQL query
        :rtype: str
        """

        sql_columns = []
        definition = self.read_yaml(file)
        print(f"Collecting Target Columns and Data Types from {file}...")
        for column in definition['target']['schema']['fields']:
            sql_columns.append(f"cast({column['source']} as {column['type']}) as {column['name']}")

        print(f"Creating source to target query...")
        query = "select "
        query += ',\n\t'.join(sql_columns)
        query += "\nfrom source_df"
        # print(f"SQL Query:\n{query}")

        return query

    def read_csv(self, file, schema, header=True, sep=',', quote='"'):
        """Function creates a Spark DataFrame based on a schema defined by a YAML config file.

        :param file: The path and filename for YAML file
        :type file: str
        :param schema: Spark schema
        :type schema: pyspark.sql.types.StructType
        :param header: Flag to process the first row of file as a header or data
        :type header: boolean
        :param sep: The delimiter character for delimited file
        :type sep: str
        :param quote: The quote character used to enclose fields within a delimited file
        :type quote: str
        :returns: Spark DataFrame
        :rtype: pyspark.sql.DataFrame
        """

        source_df = self.sparky.read.csv(header=header, path=file, schema=schema, sep=sep, quote=quote)
        print(f"Input dataframe count: {source_df.count()}")
        print('Schema:')
        source_df.printSchema()
        # source_df.show()

        return source_df

    def run_query(self, query, df):
        """Function executes a SparkSQL query

        :param query: The SparkSQL query to execute
        :type query: str
        :param df: The Spark DataFrame to execute the query against
        :type df: pyspark.sql.DataFrame
        :returns: Spark DataFrame
        :rtype: pyspark.sql.DataFrame
        """

        print(f"query:\n{query}")
        df.createOrReplaceTempView("source_df")
        target_df = self.sparky.sql(query)
        target_df.printSchema()
        # target_df.show()

        return target_df

    def check_data_quality(self, df, file):
        """Function to run data quality checks and remove them from the Spark DataFrame for further processing.
        Function returns both a valid and invalid DataFrame

        :param df: The Spark DataFrame to execute the query against
        :type df: Spark DataFrame
        :param file: The path and filename for the YAML file
        :type file: str
        :returns: Spark DataFrame
        :rtype: pyspark.sql.DataFrame
        :returns: Spark DataFrame
        :rtype: pyspark.sql.DataFrame
        """

        sql_columns = []
        definition = self.read_yaml(file)
        print(f"Collecting Data Quality Tests from {file}...")
        for column in definition['source']['file']['fields']:
            sql_columns.append(f"`{column['name']}`")

        base_query = "select "
        base_query += ',\n\t'.join(sql_columns)

        df.createOrReplaceTempView("source_df")
        bad_df = self.run_query(base_query + "\n\t, null as error \nfrom source_df \nlimit 0", df)
        # TODO Make this cleaner
        for column in definition['source']['file']['fields']:
            query = base_query
            if 'test' in column:
                query += f",\n\t'{column['name']}: {column['error']}' as error\n from source_df \n\twhere {column['test']}"
                temp_df = self.run_query(query, df)
                temp_df.show()
                bad_df = bad_df.union(temp_df)
                bad_df.show()

        bad_df.createOrReplaceTempView("bad_df")
        # bad_df.printSchema()
        # bad_df.show()

        minus_query = f"{base_query}\nfrom source_df\nminus\n{base_query}\nfrom bad_df"

        good_df = self.run_query(minus_query, df)
        # good_df.printSchema()
        # good_df.show()

        return good_df, bad_df

    # def parse_schema(self, file, object):
    #     # TODO do this once, not in each function
    #     columns = []
    #     definition = self.read_yaml(file)
    #     print(f"Collecting Schema Columns and Data Types from {file}...")
    #     for column in definition['source']['file']['fields']:

    # def write_data(self, df):
        # TODO Where ever we want the dataframes to go

    # def create_docs(self, file):
        # TODO What ever docs we can create from the configs... but probably not here in this file

