from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType


spark_jars_packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.postgresql:postgresql:42.5.4"
]


def spark_init(test_name) -> SparkSession:
    return SparkSession.builder \
        .master("local") \
        .appName(test_name) \
        .config("spark.jars.packages", ",".join(spark_jars_packages)) \
        .getOrCreate()


postgresql_settings = {
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'subscribers_restaurants',
    'user': 'student',
    'password': 'de-student',
}
kafka_security_options = {
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',

}
TOPIC_OUT = 'student.topic.cohort7.antonovams_out'  
TOPIC_IN = 'student.topic.cohort7.antonovams_in'     

def read_subscribers_restaurants(spark: SparkSession) -> DataFrame: 
    return spark.read.format('jdbc') \
        .options(**postgresql_settings) \
        .load()



schema = StructType([
    StructField("restaurant_id", StringType()),
    StructField("adv_campaign_id", StringType()),
    StructField("adv_campaign_content", StringType()),
    StructField("adv_campaign_owner", StringType()),
    StructField("adv_campaign_owner_contact", StringType()),
    StructField("adv_campaign_datetime_start", TimestampType()),
    StructField("adv_campaign_datetime_end", TimestampType()),
    StructField("datetime_created", TimestampType())
])


def read_restaurant_campaign_stream(spark: SparkSession) -> DataFrame:
    return spark.readStream.format('kafka') \
        .options(**kafka_security_options) \
        .option('subscribe', TOPIC_IN) \
        .load() \
        .select(F.col("value").cast("string").alias("value")) \
        .withColumn("json", F.from_json("value", schema)) \
        .select("json.*").filter(F.current_timestamp().between(F.col("adv_campaign_datetime_start"),F.col("adv_campaign_datetime_end")))\
        .dropDuplicates(["restaurant_id", "datetime_created"]).withWatermark("datetime_created", "10 minutes") 

def join(subscribers_df: DataFrame, restaurant_campaign_df: DataFrame) -> DataFrame:
    return subscribers_df.Join(restaurant_campaign_df, "restaurant_id") \
        .select("restaurant_id",
                "adv_campaign_id",
                "adv_campaign_content",
                "adv_campaign_owner",
                "adv_campaign_owner_contact",
                "adv_campaign_datetime_start",
                "adv_campaign_datetime_end",
                "datetime_created",
                "client_id",
                F.current_timestamp().alias("trigger_datetime_created"))


spark = spark_init('rest_campaign')
subscribers_df = read_subscribers_restaurants(spark)
restaurant_campaign_df = read_restaurant_campaign_stream(spark)
result_df = join(subscribers_df, restaurant_campaign_df)
result_df = result_df.dropDuplicates(subset=['client_id', 'adv_campaign_id']) \
                     .withWatermark('trigger_datetime_created', '10 minutes') 

def foreach_batch_func(result):
    result_df.persist()


    result \
    .select('value') \
    .write \
    .mode("append") \
    .format("kafka") \
    .options(**kafka_security_options) \
    .option("checkpointLocation", "test_query") \
    .option("topic", TOPIC_OUT).save() 
        

    result_df\
    .drop('value') \
    .withColumn('trigger_datetime_created', F.unix_timestamp(F.col('trigger_datetime_created'))) \
    .withColumn('feedback', F.lit(None).cast(StringType())) \
    .write \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://localhost:5432/jovyan') \
    .option('driver', 'org.postgresql.Driver') \
    .option('dbtable', 'subscribers_feedback') \
    .option('user', 'jovyan') \
    .option('password', 'jovyan') \
    .mode('append').save()
    

    result_df.unpersist()


res_query = result_df.writeStream \
    .trigger(processingTime='10 seconds') \
    .foreachBatch(foreach_batch_func) \
    .start()
    
res_query.awaitTermination()

