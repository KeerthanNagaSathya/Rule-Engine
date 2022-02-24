import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import logging.config
from pyspark.sql.functions import *
import datetime
import rule_generator as r

if __name__ == '__main__':
    logging.basicConfig(level="INFO")

    spark = SparkSession \
        .builder \
        .appName("Rules Engine") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    logging.info("spark session created")

    logging.info("Call the rule generator")

    process = "identification"
    key = "query_builder"
    rule_id = "rule_1"
    lookup = "false"
    table_name = ""
    valid_parameters, valid_rule_gen, message, query = r.rule_generator(spark, process, key, rule_id, lookup, table_name)

    if valid_parameters:
        if valid_rule_gen:
            with open("output/queries.txt", "w") as f:
                # f.write(query)
                f.write("\n\n")

            # tempDf = spark.sql(query)
            # logging.info(tempDf.show(truncate=False))
            # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
        else:
            logging.info(message)
    else:
        logging.info(message)
