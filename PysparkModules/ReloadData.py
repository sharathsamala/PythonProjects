
import logging
import traceback

from pyspark.sql import SparkSession

HIVE_QUERY = ""

class ReloadData(object):

    def __init__(self):
        pass

    def create_spark_session(self, application_name):

        try:
            spark = SparkSession.builder\
                .appName(application_name)\
                .config("spark.eventLog.enabled", "true") \
                .config("spark.scheduler.listenerbus.eventqueue.size", "300000") \
                .config("spark.yarn.am.waitTime", "100000") \
                .config("spark.yarn.max.executor.failures" , "2000") \
                .config("spark.akka.frameSize", "1001") \
                .config("spark.akka.threads", "32") \
                .config("sun.io.serialization.extendedDebugInfo", "true") \
                .config("spark.executor.extraJavaOptions", "-XX:-UseSplitVerifier") \
                .config("spark.shuffle.consolidateFiles", "true") \
                .config("spark.shuffle.compress", "true") \
                .config("spark.shuffle.spill.compress", "true") \
                .config("hive.exec.dynamic.partition", "true") \
                .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                .config("hive.exec.stagingdir", "/tmp/spark-hive-staging/") \
                .enableHiveSupport()\
                .getOrCreate()

            return spark
        except:
            logger.info(str(traceback.format_exc()))
            raise Exception

    def reload_data_main(self):
        logger.info("Inside main")
        spark = self.create_spark_session("project_info - (2018-08) ")
        s_logger = logging.getLogger('py4j.java_gateway')
        s_logger.setLevel(logging.ERROR)
        project_info_df = spark.sql(HIVE_QUERY.format("2018-08-01", "2018-08-10"))
        project_info_df.show(10)


logger = logging.getLogger('ReloadData')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

logger.info("Started pyspark Job...")
rd = ReloadData()
rd.reload_data_main()