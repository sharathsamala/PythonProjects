

import traceback
import getpass
import sys
import logging
import logging.handlers

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

HIVE_QUERY = """select i.workflow	, i.project_id	, i.project_name	, i.project_type	, i.project_state	, i.parent	, i.parent_name	, i.wave_type	, i.current_project_status	, i.geo_info_id	, i.fips	, i.tiger_km	, i.parent_project_status	, i.tags	, i.subtype	, i.resolution	, i.revision_alias	, i.parent_project_type	, i.todo_version	, i.root_project_id	, i.build_region_id	, i.build_region_name	, i.area_id	, i.area_name	, i.project_type_id	, i.run_count	, i.dt	, LOWER(get_json_object(r.line, '$.properties.activity.market.name')) as market, i.project_id_partition from kh_analytics.kh_project_info i join kh_analytics.kh_raw_projects r on get_json_object(r.line, '$.project_id') = i.project_id and r.project_id_partition = i.project_id_partition and r.dt = i.dt where r.dt like '%{0}%'"""


class ReloadData(object):

    def __init__(self):
        pass

    def create_spark_session(self, application_name):

        try:
            if username == "sharathsamala":
                spark = SparkSession.builder\
                    .appName(application_name)\
                    .config("spark.yarn.am.waitTime", "100000") \
                    .config("spark.yarn.max.executor.failures", "2000") \
                    .config("sun.io.serialization.extendedDebugInfo", "true") \
                    .config("spark.executor.extraJavaOptions", "-XX:-UseSplitVerifier") \
                    .config("spark.shuffle.consolidateFiles", "true") \
                    .config("spark.shuffle.compress", "true") \
                    .config("spark.shuffle.spill.compress", "true") \
                    .getOrCreate()
                return spark
            else:
                spark = SparkSession.builder\
                    .appName(application_name)\
                    .config("spark.yarn.am.waitTime", "100000") \
                    .config("spark.yarn.max.executor.failures" , "2000") \
                    .config("sun.io.serialization.extendedDebugInfo", "true") \
                    .config("spark.executor.extraJavaOptions", "-XX:-UseSplitVerifier") \
                    .config("spark.shuffle.consolidateFiles", "true") \
                    .config("spark.shuffle.compress", "true") \
                    .config("spark.shuffle.spill.compress", "true") \
                    .config("hive.exec.dynamic.partition", "true") \
                    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                    .config("hive.exec.stagingdir", "/tmp/spark-hive-staging/") \
                    .config("spark.yarn.queue", "root.kh_analytics_buildbot") \
                    .enableHiveSupport()\
                    .getOrCreate()
                return spark

        except:
            logger.info(str(traceback.format_exc()))
            raise Exception

    def fetch_market_list(self, spark):

        try:
            if username == "sharathsamala":
                return ['USA', 'ESP']
            logger.info("fetching markets list ....!")
            market_list_df = spark.sql("select distinct market from kh_analytics.kh_regions")
            market_list = market_list_df.select("market").rdd.flatMap(lambda x: x).collect()
            return market_list
        except:
            logger.error(str(traceback.format_exc()))
            return None

    def fetch_raw_project_df(self,spark,date_filter):

        try:
            logger.info("fetching raw projects  ....!")
            query = HIVE_QUERY.format(date_filter)
            logger.info("running hive query : " + str(query))
            raw_projects_df = spark.sql(query)
            return raw_projects_df
        except:
            logger.error(str(traceback.format_exc()))
            return None

    def apply_parition_logic(self, project_df, market_list):

        try:
            logger.info("Applying the partition logic to make the final df")

            def market_filter(market_name, build_region_id):

                if market_name is None:
                    if build_region_id is None:
                        return "MISIC"
                    elif "kh.eur." not in build_region_id:
                        return "USA"
                    else:
                        return "MISIC"
                elif market_name.upper() in market_list:
                    return market_name.upper()
                else:
                    return "MISIC"

            market_udf = udf(market_filter)
            logical_df = project_df.withColumn('final_market', market_udf('market', 'build_region_id'))

            final_df = logical_df.selectExpr("workflow", "project_id", "project_name", "project_type", "project_state",
                                             "parent", "parent_name", "wave_type", "current_project_status", "geo_info_id",
                                             "fips", "tiger_km", "parent_project_status", "tags", "subtype", "resolution",
                                             "revision_alias", "parent_project_type", "todo_version", "root_project_id",
                                             "build_region_id", "build_region_name", "area_id", "area_name", "project_type_id",
                                             "run_count", "dt", "final_market as market", "project_id_partition")
            final_df.show(10, False)

            final_df.repartition(1).write.mode("overwrite") \
                    .insertInto("kh_analytics_market.kh_project_info")

            return True
        except:
            logger.error(str(traceback.format_exc()))
            return False

    def reload_data_main(self, date_filter):
        try:
            logger.info("Inside main")
            logger.info("Given input argument year_month : " + str(date_filter))
            spark = self.create_spark_session("project_info - reload_with_market ")
            set_logger(spark)
            logger.info("Turned of spark info logs for better readability")

            market_list = self.fetch_market_list(spark)
            if market_list is None:
                logger.error("market list is empty closing ...!")
                raise Exception

            raw_projects_df = self.fetch_raw_project_df(spark, date_filter)
            if raw_projects_df is None:
                logger.error("Unable to fetch raw projects")
                raise Exception

            logger.info("Dropping the current partition")
            spark.sql("alter table kh_analytics_market.kh_project_info drop partition (dt='{0}')".format(date_filter))
            result = self.apply_parition_logic(raw_projects_df, market_list)
            if not result:
                logger.error("Failed to load the market partition updated data")
                raise Exception
            spark.stop()
        except :
            logger.error(str(traceback.format_exc()))
            spark.stop()
            raise Exception


def set_logger(spark):
    logger_in = spark._jvm.org.apache.log4j
    logger_in.LogManager.getLogger("org"). setLevel(logger_in.Level.ERROR )
    logger_in.LogManager.getLogger("akka").setLevel(logger_in.Level.ERROR )

logger_obj = logging.getLogger("ReloadData")
logger_obj.setLevel(logging.DEBUG)
formatter_string = "%(asctime)s - %(levelname)s - %(message)s "
formatter = logging.Formatter(formatter_string)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)
logger_obj.addHandler(console_handler)

logger = logger_obj

filter_date = None
username = getpass.getuser()
logger.info("Started pyspark Job...")


rd = ReloadData()
rd.reload_data_main("2018-08-18")
