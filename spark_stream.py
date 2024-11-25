import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')

# def create_spark_connection():
#     s_conn = None

#     try:
#         s_conn = SparkSession.builder \
#             .appName('SparkDataStreaming') \
#             .config('spark.jars.packages',
#                                            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
#                                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
#             .config('spark.cassandra.connection.host', 'localhost') \
#             .getOrCreate()

#         s_conn.sparkContext.setLogLevel("ERROR")
#         logging.info("Spark connection created successfully!")
#     except Exception as e:
#         logging.error(f"Couldn't create the spark session due to exception {e}")

#     return s_conn
def create_spark_connection():
    s_conn = None
    try:
        # s_conn = SparkSession.builder \
        #     .appName('SparkDataStreaming') \
        #     .config('spark.jars.packages', 
        #            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
        #            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        #            "org.apache.kafka:kafka-clients:3.5.1") \
        #     .config('spark.cassandra.connection.host', 'localhost') \
        #     .config("spark.driver.extraClassPath", "./kafka-clients-3.5.1.jar") \
        #     .getOrCreate()
        s_conn = SparkSession.builder \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
            .config("spark.jars", "/mnt/c/Users/U/OneDrive\ -\ exelskils/Desktop/AA\ YOUNES/ETL-PROJECTs/Realtime\ Socket\ Streaming-spark-kafka/realtime-deng-venv/lib/python3.12/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar") \
            .config("spark.driver.extraClassPath", "/mnt/c/Users/U/OneDrive\ -\ exelskils/Desktop/AA\ YOUNES/ETL-PROJECTs/Realtime\ Socket\ Streaming-spark-kafka/realtime-deng-venv/lib/python3.12/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar:/mnt/c/Users/U/OneDrive\ -\ exelskils/Desktop/AA\ YOUNES/ETL-PROJECTs/Realtime\ Socket\ Streaming-spark-kafka/realtime-deng-venv/lib/python3.12/site-packages/pyspark/jars/kafka-clients.jar") \
            .config("spark.executor.extraClassPath", "/mnt/c/Users/U/OneDrive\ -\ exelskils/Desktop/AA\ YOUNES/ETL-PROJECTs/Realtime\ Socket\ Streaming-spark-kafka/realtime-deng-venv/lib/python3.12/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar:/mnt/c/Users/U/OneDrive\ -\ exelskils/Desktop/AA\ YOUNES/ETL-PROJECTs/Realtime\ Socket\ Streaming-spark-kafka/realtime-deng-venv/lib/python3.12/site-packages/pyspark/jars/kafka-clients.jar")\
            .config("spark.cassandra.connection.host", "localhost") \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print("****************************** test *************************",sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            # streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
            #                    .option('checkpointLocation', '/tmp/checkpoint')
            #                    .option('keyspace', 'spark_streams')
            #                    .option('table', 'created_users')
            #                    .start())

            # streaming_query.awaitTermination()

            streaming_query = (selection_df.writeStream
                                  .foreachBatch(lambda batch_df, batch_id: batch_df.write
                                  .format("org.apache.spark.sql.cassandra")
                                  .mode("append")
                                  .options(table="created_users", keyspace="spark_streams")
                                  .save())
                                .option('checkpointLocation', '/tmp/checkpoint')
                                .start())

            streaming_query.awaitTermination()
