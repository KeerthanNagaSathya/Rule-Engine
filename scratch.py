"hdfs://nonpdp01/dev01/publish/bdp017/bdp017_rule_engine/data/atm_rules.json"

# Actual filtration query

" where reversal_of = 0 and reversal_by = 0 and (transaction_code in ('Deposit', 'SellFX', 'CloseAccount', 'CreditOtherBank', 'BED Cash Held Over Verification', 'BED Completed', 'CashedCheque', 'CloseAccount', 'Withdrawal') or (transaction_code = 'Exchange Funds' and (Fee_Name = 'Bank Cheque fee' or Fee_Name = 'Calling Other Bank Cheque fee')) or (transaction_code = 'Exchange Funds' and Fee_Name = '' and Bin_type = 'Cashed Items') or (transaction_code in ('Credit GL', 'GLDebit') and item_id = 620 and item_value = 'Customer')"

logging.info("Call the rule generator")

process = "filtration"
process_key = "query_lookup"
rule_id = "rule_1"
lookup = "true"
value_key = ""
table_name = "atm_transactions"
dataframes = [atm]
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
            output_df.createOrReplaceTempView("atm_filtered")
        else:
            tempDf = spark.sql(query)
            logging.info(tempDf.show(truncate=False))

        # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
    else:
        logging.info(message)
else:
    logging.info(message)

# Setting the parameters for cheque transaction lookup as part of identification.

process = "identification"
process_key = "value_lookup"
rule_id = "rule_2"
lookup = "true"
value_key = "check_cheque_transaction"
table_name = ""
dataframes = [atm]
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

# Setting the parameters for Query builder as part of identification.

process = "identification"
process_key = "query_builder"
rule_id = "rule_4"
lookup = "false"
value_key = ""
table_name = "atm_filtered"
dataframes = [atm]
apply_query = True
valid_parameters, valid_rule_gen, message, query, output_df = rule_generator(spark, process, process_key, rule_id,
                                                                             lookup,
                                                                             value_key, table_name, dataframes,
                                                                             apply_query)

logging.info("\n\n\n *************** MAIN -> IDENTIFICATION QUERY BUILDER *********************")
logging.info("valid_parameters > {}".format(valid_parameters))
logging.info("valid_rule_gen   > {}".format(valid_rule_gen))
logging.info("message          > {}".format(message))
logging.info("query            > {}".format(query))

if valid_parameters:
    if valid_rule_gen:
        logging.info("\n\n Valid Parameters and Valid rule gen, output is > {}".format(query))
        with open("output/queries.txt", "a") as f:
            f.write(str(query))
            f.write("\n\n")

        if apply_query:
            logging.info(output_df.show(truncate=False))
            output_df.createOrReplaceTempView("atm_identified")
        else:
            tempDf = spark.sql(query)
            logging.info(tempDf.show(truncate=False))

        # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")

    else:
        logging.info(message)
else:
    logging.info(message)

# Setting the parameters for Conditional as part of identification.

process = "identification"
process_key = "conditional"
rule_id = "rule_5"
lookup = "true"
value_key = ""
table_name = "atm_filtered"
dataframes = [atm]
apply_query = True
valid_parameters, valid_rule_gen, message, query, output_df = rule_generator(spark, process, process_key, rule_id,
                                                                             lookup,
                                                                             value_key, table_name, dataframes,
                                                                             apply_query)

logging.info("\n\n\n *************** MAIN -> conditional *********************")
logging.info("valid_parameters > {}".format(valid_parameters))
logging.info("valid_rule_gen   > {}".format(valid_rule_gen))
logging.info("message          > {}".format(message))
logging.info("query            > {}".format(query))

if valid_parameters:
    if valid_rule_gen:
        logging.info("\n\n Valid Parameters and Valid rule gen, output is > {}".format(query))
        with open("output/queries.txt", "w") as f:
            f.write(str(query))
            f.write("\n\n")

        if apply_query:
            logging.info(output_df.show(truncate=False))
            output_df.createOrReplaceTempView("atm_identified")
        else:
            tempDf = spark.sql(query)
            logging.info(tempDf.show(truncate=False))

        # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
    else:
        logging.info(message)
else:
    logging.info(message)