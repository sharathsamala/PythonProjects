

import subprocess
import traceback

from Logsetup import logger


SHOW_TABLES = '''hive -S -e "use {0}; show tables;"'''

class GenerateTableDDL(object):

    def __init__(self):
        self.database_name = ""


    def submit_shell_command(self, shell_cmd):

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

    def fetch_table_list(self):
        table_list = []
        try:
            logger.info("Fetching list of tables in "+self.database_name+" database")
            result = self.submit_shell_command(SHOW_TABLES.format(self.database_name))

            if result is None:
                logger.error("Error running the shell command")
                return None
            logger.info(result)
            return table_list
        except:
            logger.error(str(traceback.format_exc()))
            return None

    def generate_ddl_main(self, hive_database_name):

        try:
            logger.info("Stated DDL generations script ")
            self.database_name = hive_database_name

            result = self.fetch_table_list()
            if result is None:
                logger.info("Failed fetching list of tables")
                raise Exception


        except:
            logger.error(str(traceback.format_exc()))
            raise Exception


if __name__ == '__main__':

    Gtd = GenerateTableDDL()
    Gtd.generate_ddl_main(hive_database_name="kh_analytics")

