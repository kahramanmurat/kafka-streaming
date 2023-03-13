import csv
from time import sleep
from typing import Dict
from kafka import KafkaProducer

from settings import (
    BOOTSTRAP_SERVERS,
    INPUT_DATA_PATH_1,
    INPUT_DATA_PATH_2,
    PRODUCE_TOPIC_RIDES_CSV_1,
    PRODUCE_TOPIC_RIDES_CSV_2,
)


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print(
        "Record {} successfully produced to {} [{}] at offset {}".format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()
        )
    )


class RideCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)

    def read_records_1(self, resource_path: str):
        records, ride_keys = [], []
        i = 0
        with open(resource_path, "r") as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                # vendor_id, passenger_count, trip_distance, payment_type, total_amount
                records.append(f"{row[0]},{row[1]}, {row[2]}, {row[5]}, {row[6]}")
                ride_keys.append(str(row[0]))
                i += 1
                if i == 5:
                    break
        return zip(ride_keys, records)

    def read_records_2(self, resource_path: str):
        records, ride_keys = [], []
        i = 0
        with open(resource_path, "r") as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                # vendor_id, passenger_count, trip_distance, payment_type, total_amount
                records.append(f"{row[0]},{row[1]}, {row[2]}, {row[3]}, {row[4]}")
                ride_keys.append(str(row[0]))
                i += 1
                if i == 5:
                    break
        return zip(ride_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)


if __name__ == "__main__":
    config = {
        "bootstrap_servers": [BOOTSTRAP_SERVERS],
        "key_serializer": lambda x: x.encode("utf-8"),
        "value_serializer": lambda x: x.encode("utf-8"),
    }
    producer = RideCSVProducer(props=config)
    ride_records_1 = producer.read_records_1(resource_path=INPUT_DATA_PATH_1)
    producer.publish(topic=PRODUCE_TOPIC_RIDES_CSV_1, records=ride_records_1)
    ride_records_2 = producer.read_records_2(resource_path=INPUT_DATA_PATH_2)
    producer.publish(topic=PRODUCE_TOPIC_RIDES_CSV_2, records=ride_records_2)
