

#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'Sharath Samala'

# ################################# Module Information #################################################################
#   Module Name         : SqoopUtility
#   Purpose             : Create and Submit sqoop job based on the input configuration
#   Pre-requisites      : Logsetup.py
#   Last changed on     : 6 July 2018
#   Last changed by     : Sharath Samala
#   Reason for change   : Development
# ######################################################################################################################

# Library and external modules declaration
import sys
import json
import getopt
import os
import re
import subprocess
import traceback
import datetime
import pandas as pd
from os import environ
from datetime import timedelta
from datetime import datetime

from tabulate import tabulate
import pathos.pools as Pool

from Logsetup import logger


# CONSTANTS
MANDATORY_SOURCE_KEYS = ["name", "db_type", "hostname", "database", "username", "password", "hive_db_name", "hdfs_base_path"]
CONN_STRING = "jdbc:{0}://{1}/{2}"
SQOOP_MASTER_TEMPLATE = '''
sqoop import
$$optional_params
--connect $$connection_string
--username '$$username' --password '$$password'
--target-dir $$hdfs_base_path/$$hive_db_name/$$hive_table_name_tmp
--query "select * from ($$query)a where \$CONDITIONS"
--fields-terminated-by "\\t"
--lines-terminated-by "\\n"
--hive-import --hive-table $$hive_db_name.$$hive_table_name --hive-partition-key "load_date" --hive-partition-value $$load_date --hive-overwrite 
--split-by $$split_by
--map-column-java "$$map_columns_java"
--map-column-hive "$$map_columns_hive"
-m $$mapper_count
'''
HIVE_RETENTION_TEMPLATE = '''hive -S -e " alter table $$hive_db_name.$$hive_table_name DROP IF EXISTS PARTITION(load_date<'$$retention_date'); " '''
RETENTION_PERIOD = 7


class SqoopUtility(object):

    def __init__(self):

        self.job_configuration = None
        self.thread_pool_size = 4
        now = datetime.now()
        self.load_date = str(now.strftime("%Y-%m-%d"))

    def submit_shell_cmd(self,sqoop_cmd):
        """
        Method to run sqoop command on shell
        :param sqoop_cmd: Sqoop command to run
        :return: Status if success or error and no. of records ingested in case of success
        """
        result = {"status": "success", "message": ""}

        try:
            child = subprocess.Popen(sqoop_cmd, stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
            stream_data = child.communicate()[0]
            rc = child.returncode
            if rc != 0:
                logger.error(stream_data)
                result["status"] = "error"
                result["message"] = stream_data[-100:]
                return result
            # logger.debug(stream_data)  todo: uncomment this to see sqoop job logs in case of success

            # fetching no. of records ingested using regex
            rc_count = re.search('INFO mapreduce.ImportJobBase: Retrieved(.*)records.', stream_data[-10000:])
            if rc_count is not None:
                result["message"] = rc_count.group(1).strip()
            return result

        except Exception:
            logger.error(str(traceback.format_exc()))
            result["status"] = "error"
            result["message"] = str(traceback.format_exc())
            return result

    def fetch_json(self, path):
        """
        Method to convert json file to a python dict
        :param path: path to the json file
        :return: python dict loaded with json data, None in case of failure
        """
        try:
            json_dict = json.load(open(path))
            return json_dict
        except Exception:
            logger.error(str(traceback.format_exc()))
            return None

    def update_main_dict_env(self,main_dict,source_name):
        """
        method to update the dict with values present in environment variables
        :param main_dict: dict containing key values
        :param source_name: name of the souce to which this dict belongs
        :return: updated dict with values from environment variables
        """
        try:
            main_keys = main_dict.keys()
            for key in main_keys :
                if source_name.upper()+"_"+key.upper() in os.environ:
                    logger.info("fetching "+ key +" from environment variable: "+source_name.upper()+"_"+key.upper())
                    main_dict[key] = environ.get(source_name.upper()+"_"+key.upper())
            return main_dict
        except:
            logger.error(str(traceback.format_exc()))
            return None

    def generate_sqoop_command(self, main_dict, table_dict, source_name):
        """
        method to generate sqoop command based on the input configurations
        :param main_dict: dictionary containing the source details
        :param table_dict: dictionary containing the table configurations
        :param source_name: name of the source
        :return:
        """
        try:

            if not all(keys in str(main_dict) for keys in MANDATORY_SOURCE_KEYS):
                logger.error("one/some of the mandatory keys are not present : "+str(MANDATORY_SOURCE_KEYS))
                return None

            main_dict = self.update_main_dict_env(main_dict, source_name)
            main_dict.update(table_dict)
            main_dict["connection_string"] = CONN_STRING.format(main_dict["db_type"], main_dict["hostname"], main_dict["database"])
            main_dict["load_date"] = self.load_date
            main_dict["hdfs_base_path"] = str(main_dict["hdfs_base_path"]).rstrip("/")

            #with open(os.path.join(self.base_path, "config/master_sqoop.template"), "rb") as f: # TODO, check where to put the sqoop master template
            master_sqoop_template = SQOOP_MASTER_TEMPLATE

            for key, value in main_dict.iteritems():
                master_sqoop_template = master_sqoop_template.replace("$$"+key, value)

            final_sqoop_template = ""
            for line in master_sqoop_template.splitlines():
                if "$$" not in str(line):
                    final_sqoop_template = final_sqoop_template + line + " "

            logger.info("Generating hive query to perform retention")
            date_converted = datetime.strptime(self.load_date, '%Y-%m-%d')
            main_dict["retention_date"] = str((date_converted - timedelta(days=RETENTION_PERIOD)).strftime("%Y-%m-%d")).strip()

            logger.info("Query created for Deleting partitions older than : "+str(main_dict["retention_date"]))

            hive_retention_template = HIVE_RETENTION_TEMPLATE
            for key, value in main_dict.iteritems():
                hive_retention_template = hive_retention_template.replace("$$"+key, value)

            return final_sqoop_template, hive_retention_template

        except:
            logger.error(str(traceback.format_exc()))
            return None, None


    def run_sqoop_job(self,input_val):

        output_dict = {"table_name": "","source_name": "","status": "","record_count": "","error_traceback":""}
        try:
            input_params = input_val.split("$$")
            source_name = input_params[0]
            table_name = input_params[1]
            logger.info("Running Sqoop job for table: {} and source: {}".format(table_name,source_name))

            logger.info("reading source_configuration")
            source_dict = self.job_configuration["source_list"][source_name]["source_config"]
            logger.info("reading table configuration")
            table_dict = self.job_configuration["source_list"][source_name]["tables"][table_name]

            sqoop_cmd, hive_retention_cmd = self.generate_sqoop_command(source_dict, table_dict, source_name)

            if sqoop_cmd is None:
                logger.error("error generating sqoop command for table: {}, source: {} ".format(table_name,source_name))
                raise Exception

            logger.debug("Running Sqoop Command : "+str(sqoop_cmd))
            result = self.submit_shell_cmd(sqoop_cmd)

            output_dict["table_name"] = table_name
            output_dict["source_name"] = source_name
            if result["status"] == "success":
                output_dict["record_count"] = result["message"]
                output_dict["status"] = "success"
            else :
                output_dict["error_traceback"] = result["message"]
                output_dict["status"] = "failed"

            logger.debug("Running Hive Command for dropping older partitions: " + str(hive_retention_cmd))
            result_hive = self.submit_shell_cmd(hive_retention_cmd)

            if result_hive["status"] == "success":
                logger.info("Dropped older partitions as per the retention policy")
            else:
                logger.warn("Error occurred while dropping older partitions : " + str(result_hive["message"]))

            return output_dict

        except Exception:
            logger.error(str(traceback.format_exc()))
            return output_dict

    def run_all_sources(self):

        job_list = []
        try:
            logger.info("Running for all the sources present in the input configuration")
            logger.info("Fetching the list of sources")

            source_list = self.job_configuration["source_list"].keys()
            logger.info("total number of sources configured: " + str(len(source_list)))

            for source in source_list :
                table_list = self.job_configuration["source_list"][source]["tables"].keys()
                for table in table_list :
                    job_list.append(source+"$$"+table)

            logger.info("total number of tables configured for all the source: " + str(len(job_list)))
            logger.info("Job list : "+str(job_list))

            result = self.submit_sqoop_jobs(job_list)

            return result

        except Exception:
            logger.error(str(traceback.format_exc()))
            return False

    def run_single_source(self,source_name):

        job_list = []
        try:
            logger.info("Running for all the tables present in the given source: "+source_name)

            try:
                table_list = self.job_configuration["source_list"][source_name]["tables"].keys()
            except Exception:
                logger.error("error fetching the source and table configurations for given source: "+source_name)
                logger.error("verify is the input configurations has the correct structure and source names as passed in the arguments")
                return False

            for table in table_list:
                job_list.append(source_name+"$$"+table)

            logger.info("total number of tables configured for given source: " + str(len(job_list)))
            logger.info("Job list : "+str(job_list))

            result = self.submit_sqoop_jobs(job_list)
            return True

        except Exception:
            logger.error(str(traceback.format_exc()))
            return False

    def run_single_table(self,source_name, table_name):

        job_list = []
        try:
            logger.info("Running for the give table in the given source")

            try:
                table_list = self.job_configuration["source_list"][source_name]["tables"].keys()
            except Exception:
                logger.error("error fetching the source and table configurations for given source: "+source_name)
                logger.error("verify is the input configurations has the correct structure and source details")
                return False
            if table_name in table_list :
                job_list.append(source_name+"$$"+table_name)
            else :
                logger.error("give table: "+str(table_name)+" not found in the configurations for source: "+str(source_name))
                return False
            logger.info("Job list : "+str(job_list))

            result = self.submit_sqoop_jobs(job_list)
            return True

        except Exception:
            logger.error(str(traceback.format_exc()))
            return False

    def submit_sqoop_jobs(self,job_list):

        try:

            logger.info("Creating pool of threads based on the input job list")
            if job_list is not None :
                if len(job_list) > self.thread_pool_size :
                    pool_size = self.thread_pool_size
                else :
                    pool_size = len(job_list)

                thread_pool = Pool.ProcessPool(pool_size)

                job_list_results = thread_pool.map(self.run_sqoop_job, job_list)

                logger.info("completed running the sqoop job/jobs")
                logger.debug(job_list_results)

                df = pd.DataFrame(job_list_results)

                logger.info("Summery of all the jobs configured")
                logger.info("\n"+tabulate(df[['table_name','source_name','record_count','status','error_traceback']], headers='keys', tablefmt='psql'))

                status_list = df['status'].tolist()
            else:
                logger.error("Job list is empty, Unable to trigger any sqoop jobs")
                return False

            if "failed" in status_list :
                return False
            else:
                return True

        except Exception:
            logger.error(str(traceback.format_exc()))
            return False

    def sqoop_main(self, source_path, source_name=None,table_name=None,thread_pool=None):

        try:
            logger.info("loading source list configuration into python dict ")
            self.job_configuration = self.fetch_json(source_path)
            if self.job_configuration is None:
                logger.error("unable to load source configuration list")
                raise Exception

            if thread_pool is not None:
                logger.info("Setting up the thread pool size to the given value : "+str(thread_pool))
                self.thread_pool_size = int(thread_pool)

            if source_name is None and table_name is None:
                result = self.run_all_sources()
                if not result :
                    raise Exception

            elif table_name is None and source_name is not None :
                result = self.run_single_source(source_name)
                if not result :
                    raise Exception

            elif table_name is not None and source_name is not None :
                result = self.run_single_table(source_name,table_name)
                if not result :
                    raise Exception

            else:
                logger.error("HINT : if table is provided, Its mandatory to provide source name too...!")
                raise Exception

        except Exception:
            logger.error(str(traceback.format_exc()))
            sys.exit(1)


USAGE_STRING = """
Below is the Usage Info : 
python SqoopUtility.py -c <ingestion_config_path(JSON)> -s <source_name> -t <table_name> -p <thread_pool_size>

ingestion_config_path       : Path of the ingestion config (fileType : JSON)
source_name                 : (optional) Name of the source to ingest (will ingest all tables for this source if specific table is not provided)
table_name                  : (optional) Name of the table to ingest ot list of tables with comma seperated (EX : table1 or tabl1,table2,table3)
thread_pool_size            : (optional) No. of threads to be created (default is set to 4 )      
"""


def usage(status=1):
    sys.stdout.write(USAGE_STRING)
    sys.exit(status)


if __name__ == '__main__':

    sys.stdout.write("Started Sqoop Utility\n")

    config_path = None
    src = None
    tbl=None
    opts = None
    tp = None
    try:
        opts, args = getopt.getopt(
            sys.argv[1:], "c:s:t:p:h",
            ["ingestion_config_path=", "source_name=","table_name", "thread_pool_size", "help"])
    except Exception as e:
        sys.stderr.write("\nERROR: " + str(e))
        usage(1)

    for option, arg in opts:
        if option in ("-h", "--help"):
            usage(1)
        elif option in ("-c", "--ingestion_config_path"):
            config_path = arg
        elif option in ("-s", "--source_name"):
            src = arg
        elif option in ("-t", "--table_name"):
            tbl = arg
        elif option in ("-p", "--thread_pool_size"):
            tp = arg

    if config_path is None or config_path == "":
        sys.stderr.write("\nERROR: config_path not provided in the arguments..!")
        usage(1)

    sqoopUtil = SqoopUtility()
    result = sqoopUtil.sqoop_main(config_path, src, tbl, tp)
