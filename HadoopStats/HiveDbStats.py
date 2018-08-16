#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'Sharath Samala'


# ################################# Module Information #################################################################
#   Module Name         : HiveDbStats
#   Purpose             : To generate statistics on hive table record counts and file sizes
#   Pre-requisites      : Logsetup
# ######################################################################################################################


import sys
import os
import traceback
import json
import subprocess
import pandas as pd
import datetime
from datetime import datetime, timedelta
import pathos.pools as Pool

from Utils.Logsetup import logger

PARALLEL_POOL_SIZE = 10


class HiveDbStats(object):

    def __init__(self):
        self.input_conf_dict = {}
        self.input_date = ""
        self.date_range = ""
        self.failed_table_list = []
        self.now = datetime.now()

    def trim_hdfs_lines(self, lines):
        final_list = []
        try:
            logger.info("Fetching last "+str(self.date_range)+" days hdfs sizes for the above table ")
            line_list = lines.split("\n")[:self.date_range+3]

            for line in line_list:
                if not line.strip() == "":
                    final_list.append(line)
            return final_list
        except:
            logger.error(str(traceback.format_exc()))
            raise Exception


    def submit_shell_command(self,shell_cmd):

        try:
            child = subprocess.Popen(shell_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
            stream_data = child.communicate()[0]
            rc = child.returncode
            if rc != 0:
                logger.error(stream_data)
                return None
            if "No such file or directory" in stream_data:
                logger.error("invalid table name or hdfs command ")
                return None

            return stream_data
        except:
            logger.error(str(traceback.format_exc()))
            return None

    def fetch_date_range(self, fetch_type=None):
        try:
            logger.info("fetching the required dates")
            date_converted = datetime.strptime(self.input_date, '%Y-%m-%d')
            if fetch_type == "hdfs":
                current_month = str(date_converted.strftime("%Y-%m")).strip()
                prev_month = str((date_converted.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")).strip()
                logger.info(" Current month = "+current_month+" previous month = "+prev_month)
                return prev_month, current_month
            else:
                final_date = str((date_converted - timedelta(days=self.date_range)).strftime("%Y-%m-%d")).strip()
                logger.info(" date to be used in where condition = " + final_date)
                return final_date

        except:
            logger.error(str(traceback.format_exc()))
            if fetch_type == "hdfs":
                return None, None
            else:
                return None

    def fetch_hdfs_size(self, prev_month, curr_month):
        """
        output format : {"table_name"}
        :return:
        """

        hdfs_data_buffer = []
        try:
            table_list = self.input_conf_dict["table_list"]

            for table_vals in table_list:
                logger.info("Fetching hdfs size of table : " + table_vals["table_name"])
                hdfs_command = "hadoop fs -du -s -h " + os.path.join(
                    os.path.join(self.input_conf_dict["common_config"]["hdfs_base_path"],
                                 table_vals["table_name"].lower()),
                    self.input_conf_dict["common_config"]["partition"].strip() + "=" + curr_month + "-*")
                logger.info("Running : " + hdfs_command)

                output_result = self.submit_shell_command(hdfs_command)
                if output_result is None:
                    logger.error("Hdfs output is none")
                    return None
                hdfs_data_buffer = hdfs_data_buffer + self.trim_hdfs_lines(output_result)
                hdfs_command = "hadoop fs -du -s -h " + os.path.join(
                    os.path.join(self.input_conf_dict["common_config"]["hdfs_base_path"],
                                 table_vals["table_name"].lower()),
                    self.input_conf_dict["common_config"]["partition"].strip() + "=" + prev_month + "-*")

                output_result = self.submit_shell_command(hdfs_command)
                if output_result is None:
                    logger.error("Hdfs output is none")
                    return None
                hdfs_data_buffer = hdfs_data_buffer + self.trim_hdfs_lines(output_result)

            return hdfs_data_buffer

        except Exception:
            logger.error(str(traceback.format_exc()))
            return None

    def trim_hive_line(self,lines):
        final_list = []
        try:
            logger.info("cleaning hive output data")
            line_list = lines.split("\n")

            for line in line_list:
                if "\t" in line:
                    final_list.append(line)

            return final_list
        except:
            logger.error(str(traceback.format_exc()))
            return None

    def process_data(self, hdfs_data, hive_data):

        try:
            logger.info("Pre - Processing hdfs and hive data to load into hive")
            final_list_hdfs = []
            for line in hdfs_data:
                new_list = line.split("  ")
                try:
                    temp_path_string = new_list[2]
                    temp_path_list = temp_path_string.split("/")
                    new_list[0] = self.convert_to_gb(new_list[0])
                    new_list[1] = self.convert_to_gb(new_list[1])
                    new_list[2] = temp_path_list[-2]
                    new_list.append(temp_path_list[-1].split("dt=")[-1])
                except:
                    pass
                final_list_hdfs.append(new_list)

            final_list_hive = []
            for line in hive_data:
                new_list = line.split("\t")
                final_list_hive.append(new_list)

            hdfs_df = pd.DataFrame(final_list_hdfs)
            hdfs_df.columns = ['size_without_rep', 'size_with_rep', 'table_name', 'partition_date']

            hive_df = pd.DataFrame(final_list_hive)
            hive_df.columns = ['partition_date', 'table_name', 'rec_count']

            logger.info("Joining the hdfs and hive dataframes...")

            final_df = pd.merge(hdfs_df, hive_df, how='inner', left_on=['table_name', 'partition_date'],
                                right_on=['table_name', 'partition_date'])

            final_df['last_updated_dt'] = self.now.strftime("%Y-%m-%d %H:%M")

            final_df[['partition_date', 'table_name', 'size_with_rep', 'size_without_rep', 'rec_count',
                      'last_updated_dt']].to_csv("./output1.csv", index=False, header=False)
            logger.info("Completed writing the output file.")
            return True

        except:
            logger.error(str(traceback.format_exc()))
            return False

    def run_hive_query(self,input_dict):
        result_dict = {"status" : "success", "message" : ""}
        try:
            logger.info("Running hive query : " + input_dict["query"])
            logger.info("Query might take longer time... Please be patient....")
            result = self.submit_shell_command(input_dict["query"])

            if result is None:
                result_dict["status"] = "failed"
                result_dict["message"] = "failed due to hive error for table " + str(input_dict["table_name"])
                result_dict["table_name"] = input_dict["table_name"]
                return result_dict

            result_dict["status"] = "success"
            result_dict["message"] = result

            logger.info("completed running hive query successfully")
            return result_dict

        except:
            logger.error(str(traceback.format_exc()))
            result_dict["status"] = "failed"
            result_dict["message"] = "failed due to hive error"
            result_dict["table_name"] = input_dict["table_name"]
            return result_dict

    def fetch_record_count(self, date_condition):

        query_list = []
        hive_results = ""
        try :
            logger.info("Fetching the record count ...")
            table_list = self.input_conf_dict["table_list"]

            final_query = ''' hive -S -e " set hive.exec.parallel=true; set hive.exec.parallel.thread.number=20 ; '''
            for table_vals in table_list:
                table_query = {}
                table_query["table_name"] = table_vals["table_name"]
                query = "  select dt, '{0}' as table_name, count(*) as rec_count from {2}.{0} where dt > '{1}' group by dt ; \" ".format(table_vals["table_name"], date_condition, self.input_conf_dict["common_config"]["database_name"].strip())
                table_query["query"] = final_query + query
                query_list.append(table_query)

            pool_size = PARALLEL_POOL_SIZE
            if len(query_list) < PARALLEL_POOL_SIZE:
                pool_size = len(query_list)

            thread_pool = Pool.ProcessPool(pool_size)
            job_list_results = thread_pool.map(self.run_hive_query, query_list)

            for query_results in job_list_results:
                if query_results["status"] == "success":
                    hive_results = hive_results + "\n" + query_results["message"]
                else:
                    self.failed_table_list.append(query_results["table_name"])

            return self.trim_hive_line(hive_results)

        except :

            logger.error(str(traceback.format_exc()))
            return None

    def convert_to_gb(self, size):

        if "t" in size.lower():
            return float(str(size).split(" ")[0]) * 1000
        elif "g" in size.lower():
            return float(str(size).split(" ")[0])
        elif "k" in size.lower():
            return float(str(size).split(" ")[0]) / 1000000
        elif "m" in size.lower():
            return float(str(size).split(" ")[0]) / 1000
        else:
            return float(str(size).split(" ")[0]) / 1000000000

    def main_table_stats(self, input_config, input_date, date_range):

        try:
            logger.info("loading input configurations")
            self.input_conf_dict = json.load(open(input_config))
            self.input_date = input_date
            self.date_range = date_range

            logger.debug("Give input_dict : "+str(self.input_conf_dict))
            logger.info("dates for hdfs ")
            prev_month, current_month = self.fetch_date_range("hdfs")

            if prev_month is None or current_month is None:
                logger.info("generated dates are invalid")
                raise Exception

            hdfs_result = self.fetch_hdfs_size(prev_month, current_month)

            if hdfs_result is None or hdfs_result == "":
                raise Exception
            # logger.info(hdfs_result)
            logger.info("Fetched all hdfs results")

            date_clause = self.fetch_date_range()

            hive_result = self.fetch_record_count(date_clause)
            # logger.info(hive_result)
            if hive_result is None or hive_result == "":
                raise Exception

            logger.info("Fetched all hive results")

            final_result = self.process_data(hdfs_result, hive_result)

            if not final_result:
                raise Exception

            logger.warn("Failed table list : " + str(self.failed_table_list))
            logger.info("Compelted running the job")

        except Exception:
            logger.error(str(traceback.format_exc()))
            raise Exception



if __name__ == '__main__':
    logger.info("Started Main...")

    input_conf = "../Configs/HiveDbStatsConfig.json"
    if len(sys.argv) == 2:
        input_conf = sys.argv[1]
    inpdate = "2018-08-15"
    dtrange = 2
    if len(sys.argv) < 1:
        logger.info("Fetching the input config path")
        input_conf = sys.argv[1]

    if input_conf is None or input_conf == "":
        logger.error("input configuration path is not provided in the cl arguments")

    TS = HiveDbStats()
    TS.main_table_stats(input_conf, inpdate, dtrange)


