import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import logging.config
from pyspark.sql.functions import *
from datetime import date, datetime
# import rule_generator as r
from rule_engine.rule_generator import rule_generator

def ingest_atm_file():
    # Reading the source atm file and loading into a dataframe
    atm_df = spark.read.option("Header", "true").option("InferSchema", "true").csv("data/atm.csv")
    logging.info("Reading atm transactions csv file")
    return atm_df

if __name__ == '__main__':
    logging.basicConfig(level="INFO")

    spark = SparkSession \
        .builder \
        .appName("Rules Engine") \
        .getOrCreate()

    logging.info("spark session created")

    # Reading the source atm file and loading into a dataframe
    atm = ingest_atm_file()
    atm.createOrReplaceTempView("atm_transactions")

    logging.info("Call the rule generator")

    # Setting the parameters for filtration and calling the rule engine function to get the lookup query.

    process = "identification"
    process_key = "conditional"
    rule_id = "rule_5"
    lookup = "true"
    value_key = ""
    table_name = "atm_transactions"
    dataframes = [atm]
    valid_parameters, valid_rule_gen, message, query = rule_generator(spark, process, process_key, rule_id, lookup,
                                                                      value_key, table_name, dataframes)

    logging.info("\n\n\n *************** MAIN -> conditional *********************")
    logging.info("valid_parameters > {}".format(valid_parameters))
    logging.info("valid_rule_gen   > {}".format(valid_rule_gen))
    logging.info("message          > {}".format(message))
    logging.info("query            > {}".format(query))

    if valid_parameters:
        if valid_rule_gen:
            logging.info("valid_parameters and valid rule gen, output is >".format(query))
            with open("output/queries.txt", "w") as f:
                f.write(str(query))
                f.write("\n\n")

            # tempDf = spark.sql(query)
            # logging.info(tempDf.show(truncate=False))
            # tempDf.createOrReplaceTempView("atm_filtered")

            # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
        else:
            logging.info(message)
    else:
        logging.info(message)







