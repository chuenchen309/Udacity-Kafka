# Please complete the TODO items in the code
import asyncio
from dataclasses import dataclass, field
import json
import random
from datetime import datetime

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker


faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "org.udacity.exercise4.purchases"


async def produce(topic_name):
    """Produces data synchronously into the Kafka Topic"""
    #
    # TODO: Configure the Producer to:
    #       1. Have a Client ID
    #       2. Have a batch size of 100
    #       3. A Linger Milliseconds of 1 second
    #       4. LZ4 Compression
    #
    #       See: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    #
    p = Producer(
        {
            "bootstrap.servers": BROKER_URL,
            "compression.type": "lz4",
            "linger.ms": "1000", # 定時傳資料給broker
            "batch.num.messages": "100", # 定量傳資料給broker
            "queue.buffering.max.messages":"1000000", # buffer可容納最大訊息數量, 預設
            # "queue.buffering.max.kbytes:": "1048576" # buffer可容納最大訊息容量, 預設:1GB
        }
    )

    start_time = datetime.utcnow()
    curr_iteration = 0

    while True:
        p.produce(topic_name, f"iteration: {curr_iteration}")
        if curr_iteration % 1000000 == 0:
            elapsed = (datetime.utcnow() - start_time).seconds
            print(f"Message sent: {curr_iteration} | Total elapsed seconds: {elapsed}")
        curr_iteration += 1
        p.poll(0)

async def consume(topic_name):
    c = Consumer({
        "bootstrap.servers": BROKER_URL,
        "group.id": "0"
    })
    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        if message is None:
            print("No message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(.1)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    try:
        asyncio.run(produce_consume("com.udacity.lesson2.sample4.purchases"))
    except KeyboardInterrupt as e:
        print("shutting down")

async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


if __name__ == "__main__":
    main()
