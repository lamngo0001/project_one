#!/usr/bin/env python

import sys, json
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from flask import Flask
app = Flask(__name__)


TOPICS = ["test_topic"]


if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['one'])
    config.update(config_parser['consumer'])

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        #if True:
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    def consume():
            
        # Create Consumer instance
        consumer = Consumer(config)

        # Subscribe to topic
        consumer.subscribe(TOPICS, on_assign=reset_offset)

        # Poll for new messages from Kafka and print them.
        try:
            while True:
                msg = consumer.poll(2.5)
                if msg is None:
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    return {"Message":"Waiting..."}
                    # break;
                elif msg.error():
                    return {"ERROR: %s".format(msg.error())}
                else:
                    # Extract the (optional) key and value, and print.
                    print("Offset:")
                    return {"topic": msg.topic(), "key": msg.key(), "value": msg.value(), "offset":msg.offset()}
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            consumer.close()

    @app.route('/')
    def index():
        result = json.dumps(consume())
        print(result)
        return result
    app.run()