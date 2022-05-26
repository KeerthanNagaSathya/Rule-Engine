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


def ingest_branch_file():
    # Reading the source branch file and loading into a dataframe
    branch_df = spark.read.option("Header", "true").option("InferSchema", "true").csv("data/branch.csv")
    logging.info("Reading branch transactions csv file")
    return branch_df


if __name__ == '__main__':
    logging.basicConfig(level="INFO")

    spark = SparkSession \
        .builder \
        .appName("Rules Engine") \
        .getOrCreate()

    logging.info("spark session created")

    # Reading the source branch file and loading into a dataframe
    branch = ingest_branch_file()
    branch.createOrReplaceTempView("branch_transactions")
    branch.show(truncate=False)

    logging.info("Call the rule generator")

    # Calling the rule engine for getting the filtration query
    #---------------------------------------------------   FILTRATION ----------------------------------------------------

    process = "filtration"
    process_key = "query_lookup"
    rule_id = "filter_1"
    lookup = "true"
    value_key = ""
    table_name = "branch_transactions"
    dataframes = [branch]
    apply_query = True
    valid_parameters, valid_rule_gen, message, query, output_df = rule_generator(spark, process, process_key, rule_id,
                                                                                 lookup,
                                                                                 value_key, table_name, dataframes,
                                                                                 apply_query)

    logging.info("\n\n\n *************** MAIN -> FILTRATION *********************")
    logging.info("valid_parameters > {}".format(valid_parameters))
    logging.info("valid_rule_gen   > {}".format(valid_rule_gen))
    logging.info("message          > {}".format(message))
    logging.info("query            > {}".format(query))

    if valid_parameters:
        if valid_rule_gen:
            logging.info("valid_parameters and valid rule gen, output is >".format(query))
            with open("output/queries.txt", "w") as f:
                f.write(query)
                f.write("\n\n")

            if apply_query:
                logging.info(output_df.show(truncate=False))
                output_df.createOrReplaceTempView("branch_filtered")
            else:
                tempDf = spark.sql(query)
                logging.info(tempDf.show(truncate=False))

            # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
        else:
            logging.info(message)
    else:
        logging.info(message)

    # Calling the rule engine for getting the Fee Flag
    # ---------------------------------------------------   FEE FLAG  ----------------------------------------------------

    process = "identification"
    process_key = "value_lookup"
    rule_id = "Fee_flag"
    lookup = "true"
    value_key = "fee_flag"
    table_name = ""
    dataframes = [branch]
    apply_query = False
    valid_parameters, valid_rule_gen, message, query, output_df = rule_generator(spark, process, process_key,
                                                                                 rule_id,
                                                                                 lookup,
                                                                                 value_key, table_name, dataframes,
                                                                                 apply_query)

    logging.info("\n\n\n *************** MAIN -> IDENTIFICATION VALUE LOOKUP *********************")
    logging.info("valid_parameters > {}".format(valid_parameters))
    logging.info("valid_rule_gen   > {}".format(valid_rule_gen))
    logging.info("message          > {}".format(message))
    logging.info("query            > {}".format(query))

    if valid_parameters:
        if valid_rule_gen:

            logging.info("valid_parameters and valid rule gen, output is >".format(query))
            with open("output/queries.txt", "a") as f:
                f.write(str(query))
                f.write("\n\n")
        else:
            logging.info(message)
    else:
        logging.info(message)


    # Calling the rule engine for getting Rule_1

    process = "identification"
    process_key = "query_lookup"
    rule_id = "Rule_1"
    lookup = "true"
    value_key = ""
    table_name = "branch_filtered"
    dataframes = [branch]
    apply_query = True
    valid_parameters, valid_rule_gen, message, query, output_df = rule_generator(spark, process, process_key,
                                                                                 rule_id,
                                                                                 lookup,
                                                                                 value_key, table_name, dataframes,
                                                                                 apply_query)

    logging.info("\n\n\n *************** MAIN -> IDENTIFICATION *********************")
    logging.info("valid_parameters > {}".format(valid_parameters))
    logging.info("valid_rule_gen   > {}".format(valid_rule_gen))
    logging.info("message          > {}".format(message))
    logging.info("query            > {}".format(query))

    if valid_parameters:
        if valid_rule_gen:
            logging.info("valid_parameters and valid rule gen, output is >".format(query))
            with open("output/queries.txt", "a") as f:
                f.write(query)
                f.write("\n\n")

            if apply_query:
                logging.info(output_df.show(truncate=False))
                rule_1_df = output_df.withColumn("Rule_id", lit(rule_id))
                rule_1_df.show(truncate=False)
                rule_1_df.createOrReplaceTempView("branch_rule_1")
            else:
                tempDf = spark.sql(query)
                logging.info(tempDf.show(truncate=False))

            # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
        else:
            logging.info(message)
    else:
        logging.info(message)

    # Calling the rule engine for getting Rule_1_CT

    process = "identification"
    process_key = "query_lookup"
    rule_id = "Rule_1_CT"
    lookup = "true"
    value_key = ""
    table_name = "branch_filtered"
    dataframes = [branch]
    apply_query = True
    valid_parameters, valid_rule_gen, message, query, output_df = rule_generator(spark, process, process_key,
                                                                                 rule_id,
                                                                                 lookup,
                                                                                 value_key, table_name, dataframes,
                                                                                 apply_query)

    logging.info("\n\n\n *************** MAIN -> IDENTIFICATION *********************")
    logging.info("valid_parameters > {}".format(valid_parameters))
    logging.info("valid_rule_gen   > {}".format(valid_rule_gen))
    logging.info("message          > {}".format(message))
    logging.info("query            > {}".format(query))

    if valid_parameters:
        if valid_rule_gen:
            logging.info("valid_parameters and valid rule gen, output is >".format(query))
            with open("output/queries.txt", "a") as f:
                f.write(query)
                f.write("\n\n")

            if apply_query:
                logging.info(output_df.show(truncate=False))
                rule_1_ct_df = output_df.withColumn("Rule_id", lit(rule_id))
                rule_1_ct_df.show(truncate=False)
                rule_1_ct_df.createOrReplaceTempView("branch_rule_1_ct")
            else:
                tempDf = spark.sql(query)
                logging.info(tempDf.show(truncate=False))

            # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
        else:
            logging.info(message)
    else:
        logging.info(message)


    # Calling the rule engine for getting Rule_2

    process = "identification"
    process_key = "query_lookup"
    rule_id = "Rule_2"
    lookup = "true"
    value_key = ""
    table_name = "branch_filtered"
    dataframes = [branch]
    apply_query = True
    valid_parameters, valid_rule_gen, message, query, output_df = rule_generator(spark, process, process_key,
                                                                                 rule_id,
                                                                                 lookup,
                                                                                 value_key, table_name, dataframes,
                                                                                 apply_query)

    logging.info("\n\n\n *************** MAIN -> IDENTIFICATION *********************")
    logging.info("valid_parameters > {}".format(valid_parameters))
    logging.info("valid_rule_gen   > {}".format(valid_rule_gen))
    logging.info("message          > {}".format(message))
    logging.info("query            > {}".format(query))

    if valid_parameters:
        if valid_rule_gen:
            logging.info("valid_parameters and valid rule gen, output is >".format(query))
            with open("output/queries.txt", "a") as f:
                f.write(query)
                f.write("\n\n")

            if apply_query:
                logging.info(output_df.show(truncate=False))
                rule_2_df = output_df.withColumn("Rule_id", lit(rule_id))
                rule_2_df.show(truncate=False)
                rule_2_df.createOrReplaceTempView("branch_rule_2")
            else:
                tempDf = spark.sql(query)
                logging.info(tempDf.show(truncate=False))

            # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
        else:
            logging.info(message)
    else:
        logging.info(message)

    # Calling the rule engine for getting Rule_2_CD

    process = "identification"
    process_key = "query_lookup"
    rule_id = "Rule_2_CD"
    lookup = "true"
    value_key = ""
    table_name = "branch_filtered"
    dataframes = [branch]
    apply_query = True
    valid_parameters, valid_rule_gen, message, query, output_df = rule_generator(spark, process, process_key,
                                                                                 rule_id,
                                                                                 lookup,
                                                                                 value_key, table_name, dataframes,
                                                                                 apply_query)

    logging.info("\n\n\n *************** MAIN -> IDENTIFICATION *********************")
    logging.info("valid_parameters > {}".format(valid_parameters))
    logging.info("valid_rule_gen   > {}".format(valid_rule_gen))
    logging.info("message          > {}".format(message))
    logging.info("query            > {}".format(query))

    if valid_parameters:
        if valid_rule_gen:
            logging.info("valid_parameters and valid rule gen, output is >".format(query))
            with open("output/queries.txt", "a") as f:
                f.write(query)
                f.write("\n\n")

            if apply_query:
                logging.info(output_df.show(truncate=False))
                rule_2_cd_df = output_df.withColumn("Rule_id", lit(rule_id))
                rule_2_cd_df.show(truncate=False)
                rule_2_cd_df.createOrReplaceTempView("branch_rule_2_cd")
            else:
                tempDf = spark.sql(query)
                logging.info(tempDf.show(truncate=False))

            # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
        else:
            logging.info(message)
    else:
        logging.info(message)

    logging.info("------- All the identified dataframes -------------------")

    rule_1_df.show(truncate=False)
    rule_1_ct_df.show(truncate=False)
    rule_2_df.show(truncate=False)
    rule_2_cd_df.show(truncate=False)

    # Joining all the individually identified dataframes

    unionDF_1 = rule_1_df.union(rule_1_ct_df)
    unionDF_2 = rule_2_df.union(rule_2_cd_df)
    oos_identified = unionDF_1.union(unionDF_2)
    oos_identified.createOrReplaceTempView("oos_identified")

    oos_identified.show(truncate=False)

    oos_non_identified = spark.sql("select * from branch_filtered f where not exists (select oos.dserecn from oos_identified oos where f.dserecn = oos.dserecn)")
    oos_non_identified.show(truncate=False)


    logging.info("Run completed")