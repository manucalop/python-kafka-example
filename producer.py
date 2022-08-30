#!/usr/bin/env python
from random import choice
from typing import Optional

from confluent_kafka import Producer
from pydantic import BaseModel
from yaml import safe_load


class Config(BaseModel):
    reset: bool = False
    topics: list[str]
    default: dict
    consumer: dict

    @classmethod
    def from_yaml(cls, config_file: str) -> "Config":
        with open(config_file) as f:
            config = safe_load(f)
        config["consumer"].update(config["default"])
        return cls(**config)


class MyProducer:
    def __init__(self, config: Config):
        self.config = config
        self.producer = Producer(self.config.default)

    def produce(self, topic: str, key: Optional[str] = None, value: Optional[str] = None):
        if key is None:
            key = str(choice(range(10)))
        if value is None:
            value = str(choice(range(10)))
        self.producer.produce(
            topic,
            key=key,
            value=value,
            callback=self.delivery_callback,
        )

    def delivery_callback(self, err, msg):
        if err:
            print(f"Delivery failed: {err}")
        else:
            print(
                f" Produced Event to topic {msg.topic()}: key = {msg.key().decode('utf-8')} value = {msg.value().decode('utf-8')}"
            )

    def close(self):
        self.producer.poll(10000)
        self.producer.flush()


if __name__ == "__main__":
    config = Config.from_yaml("config.yaml")
    topic = config.topics[0]
    user_ids = ["eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"]
    products = ["book", "alarm clock", "t-shirts", "gift card", "batteries"]
    producer = MyProducer(config)

    for _ in range(10):
        key = choice(user_ids)
        value = choice(products)
        producer.produce(topic, key, value)

    producer.close()
