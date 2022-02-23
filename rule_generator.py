import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import logging.config
from pyspark.sql.functions import *
import datetime


def ingest_config():
    json_df = spark.read.option("multiline", "true").json("data/atm_rules.json")
    logging.info("reading test json from file")
    # logging.info(json_df.printSchema())
    return json_df


def ingest_atm_file():
    # Reading the source atm file and loading into a dataframe
    atm_df = spark.read.option("Header", "true").option("InferSchema", "true").csv("data/atm.csv")
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
    parentDf = rulesExplodedDf.select("id", "name", "description", "is_valid", "valid_from", "valid_till", "process", "key",
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


def rules_pipeline(pdf, cdf, in_process, in_key, in_rule_id, in_lookup, table_name, rule_vars_list):

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

    for i in pdf:  # Looping through the parent dataframe created from the json file.

        logging.info("\n\n>>> Looping through the json list")
        logging.info(
            "Parameters received are key <{}>, rule_id <{}>, is_lookup <{}>".format(in_key, in_rule_id, in_lookup))

        if check_rule:  # Checking if a part of the query is already generated, if yes then appending AND operator and continuing the procss.
            where_query = where_query + " and"
            logging.info("where_query > {}".format(where_query))

        # Fetching the details from the parent json dataframe into respective variables.

        p_id = i["id"].strip()
        p_name = i["name"].strip()
        p_desc = i["description"].strip()
        p_is_valid = i["is_valid"].strip()
        p_valid_from = i["valid_from"].strip()
        p_valid_till = i["valid_till"].strip()
        p_field_name = i["field_name"].strip()
        p_field_value = i["field_value"].strip()
        p_process = i["process"].strip()
        p_key = i["key"].strip()
        p_is_lookup = i["is_lookup"].strip()

        logging.debug("p_key <{}>".format(p_key))
        logging.debug("p_id <{}>".format(p_id))
        logging.debug("p_is_lookup <{}>".format(p_is_lookup))

        if p_process == in_process.strip() and p_key == in_key.strip() and p_id == in_rule_id.strip() and p_is_lookup == in_lookup.strip() or goto_rule_check:

            logging.info(">>>MATCH>>>\nInput parameters have a match in the json file")
            valid_params = True  # Parameters sent as input have a match with in the json file.

            if p_is_valid == "true":  # Checking if the rule is valid in the json.
                if p_valid_till != 1:  # This needs to be replaced with if current date is in between valid from and # valid till @@

                    logging.info("Rule {} is valid and is being checked".format(p_id))

                    for j in cdf:  # Looping through the child dataframe created from the json file.
                        logging.info("\n\n******** Looping through the Child dataFrame  **********")

                        if p_key == "query_lookup" or p_key == "value_lookup" or p_key == "value_lookup" or p_is_lookup == "true":
                            logging.info("Checking the lookup list as it is a lookup rule".format(p_id))
                            c_id = j["id"].strip()
                            if c_id == in_rule_id.strip():
                                c_lookup_value = j["lookup_value"].strip()
                                lookup_query = c_lookup_value
                                logging.info("lookup query > {}".format(lookup_query))
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
                    logging.info("Rule {} and {} are out of range and is skipped".format(p_valid_from, p_valid_till))
                    rule_validity = False
                    process_message = "Rule {} and {} are out of range and is skipped".format(p_valid_from,
                                                                                              p_valid_till)
                    return valid_params, rule_validity, process_message, ""

            else:
                logging.info("Rule {} is not valid and is skipped".format(p_id))
                rule_validity = False
                process_message = "Rule {} is not valid and is skipped".format(p_id)
                return valid_params, rule_validity, process_message, ""

        if rule_validity:
            logging.info("Lookup check from in_lookup > {}".format(in_lookup))
            if in_lookup.strip() == "true":
                logging.info("It is a lookup query > {}".format(lookup_query))
                total_query = select_query + lookup_query
                logging.info("total_query > {}".format(total_query))
                process_message = "Successfully returning the lookup query"
                return valid_params, rule_validity, process_message, total_query
            else:
                logging.info("It is a query builder > {}".format(total_query))
                logging.info("Rule {} is success and the field name is {} ".format(p_id, p_field_name))
                if p_field_name == "goto":
                    check_rule = True
                    goto_rule_check = True
                    check_rule_id = p_field_value
                    logging.info(
                        "\n ___GOTO___ function called, rule to be checked is <" + check_rule_id + "> and CHECK_RULE is set to true")
                    updated_rule_gen_vars = [rule_validity, check_rule, valid_params, process_message,
                                             check_rule_id,
                                             where_query, select_query,
                                             total_query, lookup_query, goto_rule_check]
                    rules_pipeline(pdf, cdf, in_process, in_key, in_rule_id, in_lookup, table_name, updated_rule_gen_vars)
                else:
                    where_query = where_query + " order by id "
                    total_query = select_query + where_query
                    process_message = "Successfully returning the dynamically generated query"
                    logging.info("\n\n COMPLETED LOOPING and outcome is <" + process_message + ">")
                    logging.info("total_query > {}".format(total_query))
                    return valid_params, rule_validity, process_message, total_query

    if valid_params:
        return valid_params, rule_validity, process_message, total_query
    else:
        rule_validity = False
        logging.info("Rules are not configured for the combination of parameters passed.")
        process_message = "Rules are not configured for the combination of parameters passed."
        return valid_params, rule_validity, process_message, ""


def rule_generator(in_process, in_key, in_rule_id, in_lookup):
    logging.info("Rule generator has been called")

    json_df = ingest_config()
    # logging.info(json_df.show(truncate=False))

    # collecting the dataframe back to the driver to pass it as a list for forming the query
    pdf, cdf = parse_json(json_df)
    # Show the schema of parent and child dataframe on console
    # logging.info(pdf.printSchema())
    # logging.info(cdf.printSchema())

    pdf_collect = pdf.collect()
    cdf_collect = cdf.collect()

    # Reading the source atm file and loading into a dataframe
    atm = ingest_atm_file()
    atm.createOrReplaceTempView("atm_transactions")

    # Pass the parent and child json dataframes to the rules_pipeline function to return the query
    rule_vars_list = initialize_variables("atm_transactions")
    valid_params, rule_validity, process_message, total_query = rules_pipeline(pdf_collect, cdf_collect, in_process, in_key,
                                                                              in_rule_id, in_lookup, "atm_transactions",
                                                                              rule_vars_list)

    return valid_params, rule_validity, process_message, total_query


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
    valid_parameters, valid_rule_gen, message, query = rule_generator(process, key, rule_id, lookup)

    if valid_parameters:
        if valid_rule_gen:
            with open("output/queries.txt", "w") as f:
                f.write(query)
                f.write("\n\n")

            tempDf = spark.sql(query)
            logging.info(tempDf.show(truncate=False))
            # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
        else:
            logging.info(message)
    else:
        logging.info(message)
