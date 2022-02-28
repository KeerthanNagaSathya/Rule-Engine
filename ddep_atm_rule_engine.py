import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import logging.config
from pyspark.sql.functions import *
from datetime import date, datetime

def rule_generator(spark, in_process, in_process_key, in_rule_id, in_lookup, in_value_key, table_name):

    logging.basicConfig(level="INFO")
    logging.info("Rule generator has been called")

    json_df = ingest_config(spark)
    # logging.info(json_df.show(truncate=False))

    # collecting the dataframe back to the driver to pass it as a list for forming the query
    pdf, cdf = parse_json(json_df)
    # Show the schema of parent and child dataframe on console
    # logging.info(pdf.printSchema())
    # logging.info(cdf.printSchema())

    pdf_collect = pdf.collect()
    cdf_collect = cdf.collect()

    # Reading the source atm file and loading into a dataframe
    atm = ingest_atm_file(spark)
    atm.createOrReplaceTempView("atm_transactions")

    # Pass the parent and child json dataframes to the rules_pipeline function to return the query
    rule_vars_list = initialize_variables(table_name)
    valid_params, rule_validity, process_message, total_query = rules_pipeline(pdf_collect, cdf_collect, in_process,
                                                                               in_process_key,
                                                                               in_rule_id, in_lookup, in_value_key,
                                                                               table_name,
                                                                               rule_vars_list)

    return valid_params, rule_validity, process_message, total_query


def ingest_config(spark):
    json_df = spark.read.option("multiline", "true").json("hdfs://nonpdp01/dev01/publish/bdp017/bdp017_rule_engine/data/atm_rules.json")
    logging.info("reading test json from file")
    # logging.info(json_df.printSchema())
    return json_df


def ingest_atm_file(spark):
    # Reading the source atm file and loading into a dataframe
    atm_df = spark.read.option("Header", "true").option("InferSchema", "true").csv("hdfs://nonpdp01/dev01/publish/bdp017/bdp017_rule_engine/data/atm.csv")
    logging.info("Reading atm transactions csv file")
    return atm_df


def parse_json(json_df):
    logging.info("Parsing Json")

    rulesExplodedDf = json_df.select(explode("rules").alias("rules")).select("rules.*")
    logging.info(rulesExplodedDf.printSchema())
    logging.info(rulesExplodedDf.show(truncate=False))

    '''
    parentDf = rulesExplodedDf.select("id", "name", "description", "is_valid", "valid_from", "valid_till", "key", "is_lookup",
                                      explode("then").alias("then")) \
        .select("id", "name", "description", "is_valid", "valid_from", "valid_till", "key", "is_lookup", "then.*")
    '''
    parentDf = rulesExplodedDf.select("id", "name", "description", "is_valid", "valid_from", "valid_till", "process",
                                      "process_key",
                                      "is_lookup", "then.*")
    logging.info(parentDf.printSchema())
    logging.info(parentDf.show(truncate=False))

    childDf = rulesExplodedDf.select("id", explode("when").alias("when")) \
        .select("id", "when.*")
    logging.info(childDf.printSchema())
    logging.info(childDf.show(truncate=False))

    return parentDf, childDf


def initialize_variables(table_name):
    # Initialization of the check and query statement variables

    rule_success = False  # This variable checks if validation of the rule id is success.
    check_rule = False  # This variable checks if a part of the query is already generated.
    valid_params = False  # This variable checks if parameters passed when passing the function are compatible with the json file.
    process_message = ""  # This variable returns the appropriate message based on the processing of rules.
    check_rule_id = 0
    where_query = " where"  # This is the initialization of the where statement
    select_query = """ select * from {} """.format(table_name)  # This is the initialization of the select statement
    total_query = ""
    lookup_query = ""
    goto_rule_check = False

    rule_gen_vars = [rule_success, check_rule, valid_params, process_message, check_rule_id, where_query, select_query,
                     total_query, lookup_query, goto_rule_check]
    return rule_gen_vars


def rules_pipeline(pdf, cdf, in_process, in_process_key, in_rule_id, in_lookup, in_value_key, table_name, rule_vars_list):

    rule_validity = rule_vars_list[0]
    check_rule = rule_vars_list[1]
    valid_params = rule_vars_list[2]
    process_message = rule_vars_list[3]
    check_rule_id = rule_vars_list[4]
    where_query = rule_vars_list[5]
    select_query = rule_vars_list[6]
    total_query = rule_vars_list[7]
    lookup_query = rule_vars_list[8]
    goto_rule_check = rule_vars_list[9]

    today = date.today()

    for i in pdf:  # Looping through the parent dataframe created from the json file.

        # Looping through the parent json dataframe.

        logging.info("\n\n>>> Looping through the json list")
        logging.info(
            "Parameters received are process_key <{}>, rule_id <{}>, is_lookup <{}>, value_key <{}>.".format(in_process_key, in_rule_id, in_lookup, in_value_key))
        logging.info("Type of i in pdf is {}".format(type(i)))

        if check_rule:  # Checking if a part of the query is already generated, if yes then appending AND operator and continuing the procss.
            where_query = where_query + " and"
            logging.info("where_query > {}".format(where_query))

        # Fetching the details from the parent json dataframe into respective variables.

        p_id = i["id"].strip()
        p_name = i["name"].strip()
        p_desc = i["description"].strip()
        p_is_valid = i["is_valid"].strip()
        p_valid_from = i["valid_from"]
        p_valid_till = i["valid_till"]
        p_field_name = i["field_name"].strip()
        p_field_value = i["field_value"]
        p_process = i["process"].strip()
        p_process_key = i["process_key"].strip()
        p_is_lookup = i["is_lookup"].strip()

        p_valid_from = datetime.strptime(p_valid_from, "%d/%m/%Y").date()
        p_valid_till = datetime.strptime(p_valid_till, "%d/%m/%Y").date()

        logging.info("p_process_key <{}>".format(p_process_key))
        logging.info("p_id <{}>".format(p_id))
        logging.info("p_is_lookup <{}>".format(p_is_lookup))

        # Checking if there is a match in the json file for the parameters passed.

        if p_process == in_process.strip() and p_process_key == in_process_key.strip() and p_id == in_rule_id.strip() and p_is_lookup == in_lookup.strip() or goto_rule_check:

            logging.info(">>>MATCH>>>\nInput parameters have a match in the json file")
            valid_params = True  # Parameters sent as input have a match with in the json file.

            # Checking if the rule is valid in the json.

            logging.info("today <{}>".format(today))

            if p_is_valid == "true":

                if today >= p_valid_from and today <= p_valid_till:  # p_valid_till != 1:  # This needs to be replaced with if current date is in between valid from and # valid till @@

                    logging.info("Rule {} is valid and is being checked".format(p_id))

                    # Looping through the child dataframe created from the json file.

                    for j in cdf:
                        logging.info("\n\n******** Looping through the Child dataFrame  **********")

                        # Checking if process key of the rule is lookup or query builder.

                        if p_process_key == "query_lookup" or p_process_key == "value_lookup" or p_process_key == "groupby_lookup" or p_is_lookup == "true":

                            # Processing of lookup.

                            logging.info("Checking the lookup list as it is a lookup rule".format(p_id))

                            c_id = j["id"].strip()
                            if c_id == in_rule_id.strip():

                                if p_process_key == "value_lookup":
                                    c_lookup_value = j["lookup_value"]
                                else:
                                    c_lookup_value = j["lookup_value"].strip()

                                lookup_query = c_lookup_value
                                logging.info("lookup query/lookup value > {}".format(lookup_query))
                                rule_validity = True
                        else:
                            c_id = j["id"]
                            c_name = j["field_name"]
                            c_value = j["field_value"]
                            c_join = j["join"]
                            c_operator = j["operator"]

                            # If a part of query is generated, then assigning the rule id that is setup in goto section of the previous rule else looping.

                            if check_rule:
                                validate_rule_id = check_rule_id
                                logging.info(
                                    "check_rule is true, A/Multiple rule/rules have already run and current rule_id is <{}>".format(
                                        validate_rule_id))
                            else:
                                validate_rule_id = p_id
                                logging.info(
                                    "check_rule is false therefore no complete rule is yet finished, Checking rule_id <{}>".format(
                                        validate_rule_id))

                            if c_id == validate_rule_id:  # Fetching the child dataframe details by mapping the corresponding parent rule_id.

                                logging.info(
                                    "\n\n *** Child row for the rule is found\n \n BUILDING THE QUERY USING THIS QUERY GENERATOR \n \n")
                                rule_validity = True  # @@

                                if not (j["join"] and j["join"].strip()) != "":
                                    logging.info("Join is empty")
                                    if c_value.isnumeric():
                                        c_value = int(c_value)

                                        if c_operator == "LIKE" or c_operator == "NOT LIKE":
                                            where_query = where_query + " {} {} {}%".format(c_name, c_operator, c_value)
                                        else:
                                            where_query = where_query + " {} {} {}".format(c_name, c_operator, c_value)
                                        logging.info("query > {}".format(where_query))
                                    else:
                                        if c_operator == "LIKE" or c_operator == "NOT LIKE":
                                            where_query = where_query + " {} {} '{}%'".format(c_name, c_operator,
                                                                                              c_value)
                                        else:
                                            where_query = where_query + " {} {} '{}'".format(c_name, c_operator,
                                                                                             c_value)
                                        logging.info("query > {}".format(where_query))
                                else:
                                    logging.info("Join is not empty")
                                    if c_value.isnumeric():
                                        c_value = int(c_value)
                                        if c_operator == "LIKE" or c_operator == "NOT LIKE":
                                            where_query = where_query + " {} {} {}%  {}".format(c_name, c_operator,
                                                                                                c_value,
                                                                                                c_join)
                                        else:
                                            where_query = where_query + " {} {} {}  {}".format(c_name, c_operator,
                                                                                               c_value,
                                                                                               c_join)
                                        logging.info("query > {}".format(where_query))
                                    else:
                                        if c_operator == "LIKE" or c_operator == "NOT LIKE":
                                            where_query = where_query + " {} {} '{}%'  {}".format(c_name, c_operator,
                                                                                                  c_value,
                                                                                                  c_join)
                                        else:
                                            where_query = where_query + " {} {} '{}'  {}".format(c_name, c_operator,
                                                                                                 c_value,
                                                                                                 c_join)
                                        logging.info("query > {}".format(where_query))
                            else:
                                logging.info("NO MATCH Found for PID <{}> and CID <{}>".format(validate_rule_id, c_id))

                    logging.info("\n\n ****** LOOPING OF THE ENTIRE CHILD DATAFRAME IS FINISHED FOR A PID *****")

                    # Checking if the parent id had a match in the child dataframe after looping through the child dataframe.

                    if rule_validity:
                        if in_lookup.strip() == "true":
                            logging.info(
                                "After looping is completed, the lookup query returned is > {}".format(lookup_query))
                        else:
                            logging.info(
                                "After looping is completed, the where query returned is > {}".format(where_query))
                            # rule_validity = True  @@
                    else:
                        logging.info("!!!! WARNING : Could not find a matching child record in json.")

                else:

                    # Rule has been configured as invalid in the date range, skipping processing.

                    logging.info("Rule {} and {} are out of range and is skipped".format(p_valid_from, p_valid_till))
                    rule_validity = False
                    process_message = "Rule {} and {} are out of range and is skipped".format(p_valid_from,
                                                                                              p_valid_till)
                    return valid_params, rule_validity, process_message, ""

            else:

                # Rule has been configured as invalid in json, skipping processing.

                logging.info("Rule {} is not valid and is skipped".format(p_id))
                rule_validity = False
                process_message = "Rule {} is not valid and is skipped".format(p_id)
                return valid_params, rule_validity, process_message, ""

        # Checking the validity of the rule after one parent and all its corresponding child records are processed.

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
                            lookup_value = lookup_query[in_value_key.strip()]  # Fetching the value for the lookup key sentfrom pyspark row
                            process_message = "Successfully returning the lookup value <{}> for the value key <{}>.".format(lookup_value, in_value_key)
                            logging.info(process_message)
                        except Exception as e:
                            process_message = "Value key {} requested not found in the json config file, please setup the value.".format(in_value_key)
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
                            process_message = "Successfully returning the list of lookup values <{}>.".format(lookup_value)
                            logging.info(process_message)
                        except Exception as e:
                            process_message = "Unable to return the list of lookup values, please validate the json file.".format(in_value_key)
                            lookup_value = ""
                            valid_params = "False"
                            logging.info(process_message)

                        logging.info("lookup_value > {}".format(lookup_value))
                        return valid_params, rule_validity, process_message, lookup_value
                else:

                    # Processing of query lookup.

                    logging.info("It is a lookup query > {}".format(lookup_query))
                    if not (table_name and table_name.strip()) != "":
                        total_query = lookup_query
                        logging.info("table name is empty, total query is > {}".format(total_query))
                    else:
                        total_query = select_query + lookup_query
                        logging.info("table name is given, total query is > {}".format(total_query))
                    process_message = "Successfully returning the lookup query"
                    return valid_params, rule_validity, process_message, total_query
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
                    logging.info(
                        "\n ___GOTO___ function called, rule to be checked is <" + check_rule_id + "> and CHECK_RULE is set to true")

                    # Creating a list of variables that need to be sent to the rules_pipeline function.

                    updated_rule_gen_vars = [rule_validity, check_rule, valid_params, process_message,
                                             check_rule_id,
                                             where_query, select_query,
                                             total_query, lookup_query, goto_rule_check]
                    rules_pipeline(pdf, cdf, in_process, in_process_key, in_rule_id, in_lookup, in_value_key, table_name,
                                   updated_rule_gen_vars)
                else:

                    # Processing of standalone rule and returning the values.

                    # where_query = where_query + " order by id "  !@@
                    if not (table_name and table_name.strip()) != "":
                        total_query = where_query
                        logging.info("table name is empty, total query is > {}".format(total_query))
                    else:
                        total_query = select_query + where_query
                        logging.info("table name is given, total query is > {}".format(total_query))

                    process_message = "Successfully returning the dynamically generated query"
                    logging.info("\n\n COMPLETED LOOPING and outcome is <" + process_message + ">")
                    logging.info("total_query > {}".format(total_query))
                    return valid_params, rule_validity, process_message, total_query

    # After processing of json dataframes, checking if parameters are valid.

    if valid_params:
        return valid_params, rule_validity, process_message, total_query
    else:

        # Returning the appropriate message if rules are not configured for the parameters passed.

        rule_validity = False
        logging.info("Rules are not configured for the combination of parameters passed.")
        process_message = "Rules are not configured for the combination of parameters passed."
        return valid_params, rule_validity, process_message, ""


if __name__ == '__main__':
    logging.basicConfig(level="INFO")

    spark = SparkSession \
        .builder \
        .appName("Rules Engine") \
        .getOrCreate()

    logging.info("spark session created")

    logging.info("Call the rule generator")

    # Setting the parameters for filtration and calling the rule engine function to get the lookup query.

    process = "filtration"
    process_key = "query_lookup"
    rule_id = "rule_3"
    lookup = "true"
    value_key = ""
    table_name = "atm_transactions"
    valid_parameters, valid_rule_gen, message, query = rule_generator(spark, process, process_key, rule_id, lookup, value_key, table_name)

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
    valid_parameters, valid_rule_gen, message, query = rule_generator(spark, process, process_key, rule_id, lookup,
                                                                      value_key, table_name)

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
    rule_id = "rule_1"
    lookup = "false"
    value_key = ""
    table_name = "atm_filtered"
    valid_parameters, valid_rule_gen, message, query = rule_generator(spark, process, process_key, rule_id, lookup,
                                                                      value_key, table_name)

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







