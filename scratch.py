"hdfs://nonpdp01/dev01/publish/bdp017/bdp017_rule_engine/data/atm_rules.json"

    process = "filtration"
    process_key = "query_lookup"
    rule_id = "rule_1"
    lookup = "true"
    value_key = ""
    table_name = "atm_transactions"
    dataframes = [atm]
    valid_parameters, valid_rule_gen, message, query = rule_generator(spark, process, process_key, rule_id, lookup, value_key, table_name, dataframes)

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

            tempDf = spark.sql(query)
            logging.info(tempDf.show(truncate=False))
            tempDf.createOrReplaceTempView("atm_filtered")

            # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
        else:
            logging.info(message)
    else:
        logging.info(message)




    # Setting the parameters for identification and calling the rule engine function to get the query_builder.

    process = "identification"
    process_key = "value_lookup"
    rule_id = "rule_2"
    lookup = "true"
    value_key = "check_cheque_transaction"
    table_name = ""
    dataframes = [atm]
    valid_parameters, valid_rule_gen, message, query = rule_generator(spark, process, process_key, rule_id, lookup,
                                                                      value_key, table_name, dataframes)

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

            # tempDf = spark.sql(query)
            # logging.info(tempDf.show(truncate=False))
            # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
        else:
            logging.info(message)
    else:
        logging.info(message)

    # Setting the parameters for identification and calling the rule engine function to get the query.

    process = "identification"
    process_key = "query_builder"
    rule_id = "rule_3"
    lookup = "false"
    value_key = ""
    table_name = "atm_transactions"
    dataframes = [atm]
    valid_parameters, valid_rule_gen, message, query = rule_generator(spark, process, process_key, rule_id, lookup,
                                                                      value_key, table_name, dataframes)

    logging.info("\n\n\n *************** MAIN -> IDENTIFICATION QUERY BUILDER *********************")
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

            # tempDf2 = spark.sql("select * from atm_filtered  where transaction_total_amount >= 10000")
            tempDf2 = spark.sql(query)
            logging.info(tempDf2.show(truncate=False))
            # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
        else:
            logging.info(message)
    else:
        logging.info(message)



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













# Checking the validity of the rule after one parent and all its corresponding child records are processed.

            logging.info(
                "\n\n ________________  Looping of parent dataframe is finished, checking rule validity <{}> ____________ ".format(
                    rule_validity))

            if rule_validity:
                logging.info("Lookup check from in_lookup > {}".format(in_lookup))

                # Checking if the rule is a lookup rule or a query builder.

                if in_lookup.strip() == "true":

                    if in_process_key.strip() == "value_lookup":

                        # Processing of value lookup.

                        logging.info("It is a lookup value > {}".format(lookup_query))
                        logging.info("Type of lookup value is {}".format(type(lookup_query)))
                        logging.info("lookup_value requested for a value key> {}".format(in_value_key))

                        # Checking if a key has been passed as parameter to get the specific lookup value.

                        if (in_value_key and in_value_key.strip()) != "":

                            # Processing of value lookup with a when a value key is passed as parameter.

                            try:
                                # lookup_query = lookup_query.asDict()  # Commenting the conversion of pyspark row to dict as sometimes it is returned as string
                                lookup_query = json.loads(lookup_query)
                                logging.info("Type of lookup value is {}".format(type(lookup_query)))
                                lookup_value = lookup_query[
                                    in_value_key.strip()]  # Fetching the value for the lookup key sentfrom pyspark row
                                process_message = "Successfully returning the lookup value <{}> for the value key <{}>.".format(
                                    lookup_value, in_value_key)
                                logging.info(process_message)
                            except Exception as e:
                                process_message = "Value key {} requested not found in the json config file, please setup the value.".format(
                                    in_value_key)
                                lookup_value = ""
                                valid_params = "False"
                                logging.info(process_message)

                            logging.info("Value fetched for the in_value_key is {}".format(lookup_value))
                            return valid_params, rule_validity, process_message, lookup_value

                        else:

                            # Processing of value lookup without a specific value key.

                            try:
                                # lookup_query = lookup_query.asDict()  # Commenting the conversion of pyspark row to dict as sometimes it is returned as string
                                lookup_query = json.loads(lookup_query)
                                lookup_value = [(k, v) for k, v in
                                                lookup_query.items()]  # Converting the dict to list and returning the set of values
                                logging.info("lookup_value type > {}".format(type(lookup_value)))
                                process_message = "Successfully returning the list of lookup values <{}>.".format(
                                    lookup_value)
                                logging.info(process_message)
                            except Exception as e:
                                process_message = "Unable to return the list of lookup values, please validate the json file.".format(
                                    in_value_key)
                                lookup_value = ""
                                valid_params = "False"
                                logging.info(process_message)

                            logging.info("lookup_value > {}".format(lookup_value))
                            return valid_params, rule_validity, process_message, lookup_value

                    elif in_process_key.strip() == "query_lookup":

                        # Processing of query lookup.

                        logging.info("It is a lookup query > {}".format(lookup_query))
                        if not (in_table_name and in_table_name.strip()) != "":
                            total_query = lookup_query
                            logging.info("table name is empty, total query is > {}".format(total_query))
                        else:
                            total_query = select_query + lookup_query
                            logging.info("table name is given, total query is > {}".format(total_query))
                        process_message = "Successfully returning the lookup query"
                        return valid_params, rule_validity, process_message, total_query

                    elif in_process_key.strip() == "conditional":

                        # Processing of conditional lookup.

                        if p_field_name == "goto":

                            check_rule = True
                            goto_rule_check = True
                            check_rule_id = p_field_value
                            logging.info(
                                "\n ___GOTO___ function called, rule to be checked is <" + check_rule_id + "> and CHECK_RULE is set to true")

                            # Creating a list of variables that need to be sent to the rules_pipeline function.

                            updated_rule_gen_vars = [rule_validity, check_rule, valid_params, process_message,
                                                     check_rule_id,
                                                     where_query, select_query,
                                                     total_query, lookup_query, goto_rule_check]

                            in_lookup = ""
                            in_process_key = "query_builder"
                            in_rule_id = check_rule_id
                            rule_validity = False
                            rules_pipeline(pdf, cdf, in_process, in_process_key, in_rule_id, in_lookup, in_value_key,
                                           in_table_name,
                                           updated_rule_gen_vars)
                        else:

                            process_message = "Conditional process without proper condition setup.".format(
                                p_field_name)
                            lookup_value = lookup_query
                            valid_params = "False"
                            logging.info(process_message)
                            return valid_params, rule_validity, process_message, lookup_value

                else:

                    # Processing of query builder.

                    logging.info("It is a query builder > {}".format(total_query))
                    logging.info("Rule {} is success and the field name is {} ".format(p_id, p_field_name))

                    # Checking if the rule is standalone or has goto option to another rule.

                    if p_field_name == "goto":

                        # Processing of rule cascading as it has a goto option.

                        check_rule = True
                        goto_rule_check = True
                        check_rule_id = p_field_value
                        in_rule_id = check_rule_id
                        rule_validity = False
                        logging.info(
                            "\n ___GOTO___ function called, rule to be checked is <" + check_rule_id + "> and CHECK_RULE is set to true")

                        # Creating a list of variables that need to be sent to the rules_pipeline function.

                        updated_rule_gen_vars = [rule_validity, check_rule, valid_params, process_message,
                                                 check_rule_id,
                                                 where_query, select_query,
                                                 total_query, lookup_query, goto_rule_check]
                        rules_pipeline(pdf, cdf, in_process, in_process_key, in_rule_id, in_lookup, in_value_key,
                                       in_table_name,
                                       updated_rule_gen_vars)
                    else:

                        # Processing of standalone rule and returning the values.

                        # where_query = where_query + " order by id "  !@@
                        if not (in_table_name and in_table_name.strip()) != "":
                            total_query = where_query
                            logging.info("table name is empty, total query is > {}".format(total_query))
                        else:
                            total_query = select_query + where_query
                            logging.info("table name is given, total query is > {}".format(total_query))

                        process_message = "Successfully returning the dynamically generated query"
                        logging.info("\n\n COMPLETED LOOPING and outcome is <" + process_message + ">")
                        logging.info("total_query > {}".format(total_query))
                        return valid_params, rule_validity, process_message, total_query