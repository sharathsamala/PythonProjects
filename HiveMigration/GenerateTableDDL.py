

import subprocess
import traceback
import os

from Utils.Logsetup import logger


SHOW_TABLES = '''hive -S -e "use {0}; show tables;"'''
SHOW_CREATE_TABLE = '''hive -S -e "use {0}; show create table {1};"'''
FILE_NAME = "./hive_ddl_output.hql"
REPLACE_FROM = "pv34-neutron-prod"
REPLACE_WITH = "sg33-basemap01-prod"

class GenerateTableDDL(object):

    def __init__(self):
        self.database_name = ""
        if os.path.exists(FILE_NAME):
            os.remove(FILE_NAME)


    def submit_shell_command(self, shell_cmd):

        try:
            logger.info("Running shell command : " + shell_cmd)
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

    def fetch_table_list(self):
        table_list = []
        try:
            logger.info("Fetching list of tables in "+self.database_name+" database")
            result = self.submit_shell_command(SHOW_TABLES.format(self.database_name))

            if result is None:
                logger.error("Error running the shell command")
                return None
            result_lines = result.split("\n")
            logger.debug(result_lines)

            for line in result_lines:
                line_list = line.split(" ")
                if len(line_list) > 1 or line_list[0] == "":
                    pass
                else:
                    table_list.append(line.split(" ")[0])
            return table_list
        except:
            logger.error(str(traceback.format_exc()))
            return None

    def write_to_file(self, table_ddl, table_name):

        try:
            result_lines = table_ddl.split("\n")
            with open(FILE_NAME, "a") as f:

                flag = False
                f.write("\n\n")
                for line in result_lines:
                    if flag:
                        f.write(line.strip().replace(REPLACE_FROM, REPLACE_WITH)+" ")
                    if "CREATE " in line.strip():
                        f.write(line.strip() + " ")
                        flag = True
                    if "TBLPROPERTIES " in line.strip():
                        flag = False

                f.write("; ")
                f.write("\nmsck repair table {0} ; ".format(table_name))
            return True
        except:
            logger.error(str(traceback.format_exc()))
            return False

    def generate_table_ddl(self, table_list):

        try:
            for table in table_list:
                hive_create_table = SHOW_CREATE_TABLE.format(self.database_name, table)
                result = self.submit_shell_command(hive_create_table)

                if result is None:
                    logger.info("Unable to fetch the create table for : " + table)
                    return False

                result = self.write_to_file(result, table)

                if not result:
                    logger.error("Failed generating ddl for table : " + table)

        except:
            logger.error(str(traceback.format_exc()))
            return False

    def generate_ddl_main(self, hive_database_name):

        try:
            logger.info("Stated DDL generations script ")
            self.database_name = hive_database_name

            result = self.fetch_table_list()
            if result is None:
                logger.info("Failed fetching list of tables")
                raise Exception
            logger.info("Table list : " + str(result))

            result = self.generate_table_ddl(result)

            return result

        except:
            logger.error(str(traceback.format_exc()))
            raise Exception


if __name__ == '__main__':

    Gtd = GenerateTableDDL()
    Gtd.generate_ddl_main(hive_database_name="kh_analytics")

