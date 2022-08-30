from typing import Optional

from confluent_kafka import OFFSET_BEGINNING, Consumer
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


class MyConsumer:
    def __init__(self, config: Config):
        self.config = config
        self.consumer = Consumer(self.config.consumer)
        self.consumer.subscribe(
            self.config.topics,
        )

    def consume(self):
        msg = self.consumer.poll(1.0)
        if msg is None:
            print("Waiting...")
            return None
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            return None
        print(
            f"Consumed event from topic {msg.topic()}: key = {msg.key().decode('utf-8')} value = {msg.value().decode('utf-8')}"
        )

    def close(self):
        self.consumer.close()

    def run(self):
        while True:
            try:
                self.consume()
            except KeyboardInterrupt:
                self.close()
                break


if __name__ == "__main__":
    config = Config.from_yaml("config.yaml")
    consumer = MyConsumer(config)
    consumer.run()
