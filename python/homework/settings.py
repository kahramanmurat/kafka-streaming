import pyspark.sql.types as T

INPUT_DATA_PATH = "../resources/green_tripdata_2019-01.csv"
INPUT_DATA_PATH_FHV = "../resources/fhv_tripdata_2019-01.csv"
BOOTSTRAP_SERVERS = "localhost:9092"

INPUT_DATA_PATH_1 = "../resources/green_tripdata_2019-01.csv"
INPUT_DATA_PATH_2 = "../resources/fhv_tripdata_2019-01.csv"
PRODUCE_TOPIC_RIDES_CSV_1 = "rides_green"
PRODUCE_TOPIC_RIDES_CSV_2 = "rides_fhv"

TOPIC_WINDOWED_VENDOR_ID_COUNT = "vendor_counts_windowed"

PRODUCE_TOPIC_RIDES_CSV = consume_topic_green = "rides_green"
PRODUCE_TOPIC_RIDES_CSV_FHV = consume_topic_fhv = "rides_fhv"

RIDE_SCHEMA = T.StructType(
    [
        T.StructField("vendor_id", T.StringType()),
        T.StructField("lpep_pickup_datetime", T.TimestampType()),
        T.StructField("lpep_dropoff_datetime", T.TimestampType()),
        T.StructField("PULocationID", T.IntegerType()),
        T.StructField("DOLocationID", T.IntegerType()),
    ]
)

consume_topic_green_schema = T.StructType(
    [
        T.StructField("VendorID", T.StringType(), True),
        T.StructField("lpep_pickup_datetime", T.TimestampType(), True),
        T.StructField("lpep_dropoff_datetime", T.TimestampType(), True),
        T.StructField("PULocationID", T.StringType(), True),
        T.StructField("DOLocationID", T.StringType(), True),
    ]
)


consume_topic_fhv_schema = T.StructType(
    [
        T.StructField("dispatching_base_num", T.StringType(), True),
        T.StructField("pickup_datetime", T.TimestampType(), True),
        T.StructField("dropOff_datetime", T.TimestampType(), True),
        T.StructField("PULocationID", T.StringType(), True),
        T.StructField("DOLocationID", T.StringType(), True),
    ]
)
