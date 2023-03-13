from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from settings import (
    consume_topic_green,
    consume_topic_fhv,
    consume_topic_green_schema,
    consume_topic_fhv_schema,
)


def read_from_kafka(consume_topic: str):
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092")
        .option("subscribe", consume_topic)
        .option("startingOffsets", "earliest")
        .option("checkpointLocation", "checkpoint")
        .load()
    )
    return df_stream


def parse_ride_from_kafka_message(df, schema):
    """take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema"""
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # split attributes to nested array in one Column
    col = F.split(df["value"], ", ")

    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


def sink_console(df, output_mode: str = "complete", processing_time: str = "5 seconds"):
    write_query = (
        df.writeStream.outputMode(output_mode)
        .trigger(processingTime=processing_time)
        .format("console")
        .option("truncate", False)
        .start()
    )
    return write_query  # pyspark.sql.streaming.StreamingQuery


def sink_memory(df, query_name, query_template):
    query_df = df.writeStream.queryName(query_name).format("memory").start()
    query_str = query_template.format(table_name=query_name)
    query_results = spark.sql(query_str)
    return query_results, query_df


def sink_kafka(df, topic):
    write_query = (
        df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092")
        .outputMode("complete")
        .option("topic", topic)
        .option("checkpointLocation", "checkpoint")
        .start()
    )
    return write_query


def prepare_df_to_kafka_sink(df, value_columns, key_column=None):
    columns = df.columns

    df = df.withColumn("value", F.concat_ws(", ", *value_columns))
    if key_column:
        df = df.withColumnRenamed(key_column, "key")
        df = df.withColumn("key", df.key.cast("string"))

    return df.select(["key", "value"])


def op_groupby(df, column_names):
    df_aggregation = df.groupBy(column_names).count()
    return df_aggregation


# def op_windowed_groupby(df, window_duration, slide_duration):
#     df_windowed_aggregation = df.groupBy(
#         F.window(
#             timeColumn=df.pickup_datetime,
#             windowDuration=window_duration,
#             slideDuration=slide_duration,
#         ),
#         df.PULocationID,
#     ).count()

#     return df_windowed_aggregation


if __name__ == "__main__":
    spark = SparkSession.builder.appName("streaming-examples").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # read_streaming green data
    consume_topic_green_df = read_from_kafka(consume_topic=consume_topic_green)
    print(consume_topic_green_df.printSchema())

    # read_streaming fhv data
    consume_topic_fhv_df = read_from_kafka(consume_topic=consume_topic_fhv)
    print(consume_topic_fhv_df.printSchema())

    # parse streaming green data
    df_rides_green = parse_ride_from_kafka_message(
        consume_topic_green_df, consume_topic_green_schema
    )

    df_rides_green = df_rides_green.withColumnRenamed("VendorID", "vendor_id")
    df_rides_green = df_rides_green.withColumnRenamed(
        "lpep_pickup_datetime", "pickup_datetime"
    )
    df_rides_green = df_rides_green.withColumnRenamed(
        "lpep_dropoff_datetime", "dropoff_datetime"
    )

    print(df_rides_green.printSchema())

    # parse streaming fhv data
    df_rides_fhv = parse_ride_from_kafka_message(
        consume_topic_fhv_df, consume_topic_fhv_schema
    )

    df_rides_fhv = df_rides_fhv.withColumnRenamed("dispatching_base_num", "vendor_id")
    df_rides_fhv = df_rides_fhv.withColumnRenamed(
        "dropOff_datetime", "dropoff_datetime"
    )

    print(df_rides_fhv.printSchema())

    # Combine the two dataframes using union
    combined_df = df_rides_green.union(df_rides_fhv)
    print(combined_df.printSchema())

    sink_console(combined_df, output_mode="append")

    df_trip_count_by_PULocationID = op_groupby(combined_df, ["PULocationID"])
    # df_trip_count_by_pickup_date_PULocationID = op_windowed_groupby(
    #     combined_df, window_duration="10 minutes", slide_duration="5 minutes"
    # )

    # write the output out to the console for debugging / testing
    sink_console(df_trip_count_by_PULocationID)
    # write the output to the kafka topic
    df_count_messages = prepare_df_to_kafka_sink(
        df=df_trip_count_by_PULocationID,
        value_columns=["count"],
        key_column="PULocationID",
    ).orderBy(F.desc("count"))

    kafka_sink_query = sink_kafka(df=df_count_messages, topic="rides_all")

    spark.streams.awaitAnyTermination()
