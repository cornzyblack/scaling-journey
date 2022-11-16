#!/usr/bin/env python

from time import sleep
from random import randint
from utils import get_env_variables, get_config_dict
from utils import CONFLUENT_KAFKA_SERVICE, LOCAL_KAFKA_SERVICE
from utils import create_producer, EventGenerator, convert_event_to_bytes

if __name__ == '__main__':
    envs = get_env_variables()
    topic = envs["TOPIC"]
    delay = int(envs["DELAY"])
    service = envs["SERVICE"]
    no_events = int(envs["NUM_EVENT_KEYS"])
    config_dict = get_config_dict(envs["CONFIG_FILENAME"])

    producer = create_producer(
        config_dict=config_dict, service=service)

    try:
        while True:
            # If events are less than 5, up that number by adding 5
            if no_events < 10:
                no_events = 10
            # Randomly generate a random number between 5 and the number of events
            random_no_events = randint(10, no_events)
            event_obj = EventGenerator(random_no_events)
            for event in event_obj.events:
                topic_data = convert_event_to_bytes(topic, event)
                if service == LOCAL_KAFKA_SERVICE:
                    producer.send(**topic_data)
                    producer.flush()
                if service == CONFLUENT_KAFKA_SERVICE:
                    producer.produce(**topic_data)
                    producer.poll(1)
            # Sleep 45 seconds before sending next events
            sleep(delay)
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        producer.close()
