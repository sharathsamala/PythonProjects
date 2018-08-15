import pyodbc
import traceback
import pandas

import CONSTANTS
from Logsetup import logger


class MsSqldbUtility(object):

    def __init__(self):
        self.connection= None
        self.cursor = None

    def main(self):
        pass

    def close_conn(self,conn_obj):
        result={}
        try :

            if conn_obj is not None:
                # Checking type of the connection passed before closing
                if isinstance(conn_obj, pyodbc.Connection):
                    conn_obj.close()
                    logger.info("closed connection successfully")
                else:
                    status_message = "The Connection object passed is not valid"
                    raise Exception(status_message)

            result = {CONSTANTS.STATUS_KEY: CONSTANTS.STATUS_SUCCESS}
            return result

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            status_message = "ERROR : " + str(traceback.format_exc())
            logger.error(status_message)
            conn_result = {CONSTANTS.STATUS_KEY: CONSTANTS.STATUS_FAILED, CONSTANTS.ERROR_KEY: str(e)}
            return conn_result


    def execute_query(self,query,conn_obj):
        dict_result = {}
        fetched_data = {}
        try :
            logger.debug("checking if connection is null or empty")
            if conn_obj is None or str(conn_obj) == "" :
                logger.error("empty connection object or invalid connection")
                raise Exception
            else :
                logger.debug("Executing the Given Sql query : "+query)

                try:
                    fetched_data = pandas.read_sql(query, conn_obj)
                except Exception as e:
                    if str(e) == "'NoneType' object is not iterable":
                        status_message = "No data is returned for query " + query.decode('utf-8') \
                                         + "executing the next query."
                        logger.debug(status_message)
                    else:
                        status_message = "Error occurred while executing the query " + query.decode('utf-8')
                        raise e
                # Formulating the output dictionary based on the number of the records in the data frame
                # Checking for the number of records
                if len(fetched_data) > 0:
                    fetched_data_dict = fetched_data.to_dict(orient='dict')
                    dict_result[CONSTANTS.RESULT_KEY] = fetched_data_dict
                else:
                    dict_result[CONSTANTS.RESULT_KEY] = None
                dict_result[CONSTANTS.STATUS_KEY] = CONSTANTS.STATUS_SUCCESS
                result_dictionary = dict_result
                status_message = "Query has been successfully executed"
                logger.debug(status_message)

                return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            status_message = "ERROR : " + str(traceback.format_exc())
            logger.error(status_message)
            conn_result = {CONSTANTS.STATUS_KEY: CONSTANTS.STATUS_FAILED, CONSTANTS.ERROR_KEY: str(e)}
            return conn_result

    def create_conn(self,connection_details):
        status_message = ""
        try :
            status_message="creating azure-ms-sql connection"
            logger.info(status_message)
            server_name = connection_details["server_name"]
            database_name= connection_details["database_name"]
            username= connection_details["username"]
            password = connection_details["password"]
            port = connection_details["port"]
            mssql_driver_string = connection_details["mssql_driver_string"]

            conn = pyodbc.connect('DRIVER={'+mssql_driver_string+'};SERVER='+server_name+
                                  ';PORT='+port+';DATABASE='+database_name+';UID='+
                                  username+';PWD='+ password)
            conn.autocommit = True
            status_message="successfully connected to the given server"
            logger.info(status_message)

            conn_result = {CONSTANTS.STATUS_KEY: CONSTANTS.STATUS_SUCCESS, CONSTANTS.RESULT_KEY: conn}

            return conn_result

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            status_message = "ERROR : " + str(traceback.format_exc())
            logger.error(status_message)
            conn_result = {CONSTANTS.STATUS_KEY: CONSTANTS.STATUS_FAILED, CONSTANTS.ERROR_KEY: str(e)}
            return conn_result

    def create_mssql_conn(self,connection_details):
        status_message = ""
        try :
            status_message="creating ms-sql connection"
            logger.info(status_message)
            server_name = connection_details["server_name"]
            database_name= connection_details["database_name"]
            username= connection_details["username"]
            password = connection_details["password"]
            port = connection_details["port"]

            conn = pymssql.connect(server=server_name,user=username,password=password,database=database_name,port=port)
            conn.autocommit = True
            status_message="successfully connected to the given server"
            logger.info(status_message)

            conn_result = {CONSTANTS.STATUS_KEY: CONSTANTS.STATUS_SUCCESS, CONSTANTS.RESULT_KEY: conn}

            return conn_result

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            status_message = "ERROR : " + str(traceback.format_exc())
            logger.error(status_message)
            conn_result = {CONSTANTS.STATUS_KEY: CONSTANTS.STATUS_FAILED, CONSTANTS.ERROR_KEY: str(e)}
            return conn_result


if __name__ == '__main__':
    print("Started SqlServer Utility")

    sqlUtl = MsSqldbUtility()
    status = sqlUtl.main()

