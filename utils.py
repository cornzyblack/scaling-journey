from kafka import KafkaProducer, KafkaConsumer
from confluent_kafka import Producer
from confluent_kafka import Consumer
from random import randint, sample, choice, shuffle
from datetime import datetime, timedelta
from typing import Any
import string
import json
from dotenv import dotenv_values
from configparser import ConfigParser

CONFLUENT_KAFKA_SERVICE: str = "confluent"
LOCAL_KAFKA_SERVICE: str = "local"

FORM_SAMPLE: dict[str, Any] = {
    "id": None,
    "type": None,
    "title": "my test form",
    "settings": {
        "language": "en",
        "progress_bar": "proportion",
        "show_time_to_complete": True,
        "show_number_of_submissions": False,
        "redirect_after_submit_url": "https://www.google.com",
    },

    "fields": [{
        "id": "Auxd6Itc4qgK",
        "title": "What is your name?",
        "reference": "01FKQZ2EK4K7SZS388XF5GA945",
        "validations": {
            "required": False
        },
        "type": "open_text",
        "attachment": {

            "type": "image",
            "href": "https://images.typeform.com/images/WMALzu59xbXQ"
        }

    },
        {

        "id": "KFKzcZmvZfxn",
        "title": "What is your phone number?",
        "reference": "9e5ecf29-ee84-4511-a3e4-39805412f8c6",
        "properties": {
            "default_country_code": "us"
        },
        "validations": {
            "required": False
        },
        "type": "phone_number"
    }]
}


def get_env_variables() -> dict:
    """Gets the topic and service to use

    Returns:
        dict: A dictionary with the topic and service to use
    """
    envs = dotenv_values("development.env")

    # Cast to Integer
    if envs:
        if isinstance(envs.get("NUM_EVENT_KEYS"), str):
            envs["NUM_EVENT_KEYS"] = int(envs["NUM_EVENT_KEYS"])
    return envs


def get_config_dict(file_name: str = "config.ini") -> dict:
    """Creates a config in dict format

    Args:
        file_name (str): The name of the config file.  Defaults to config.ini

    Returns:
        dict: A config dictionary
    """
    config_dict: dict[str, dict] = {}
    config_parser = ConfigParser()

    with open(file_name, "r", encoding="utf8") as file_obj:
        raw_config = file_obj.read()
        config_parser.read_string(raw_config)
        config_dict = {}
        config_dict["consumer"] = dict(config_parser['consumer'])
        config_dict["consumer"].update(
            dict(config_parser['default']))
        config_dict["producer"] = dict(config_parser['producer'])
        config_dict["producer"].update(
            dict(config_parser['default']))
    return config_dict


class EventGenerator():
    """
        An Event generator class that generates random events
    """

    def __init__(self, num_event_keys, happened_at=datetime.now()):
        state_1 = sample(["edited", "published"], counts=[2, 1], k=3)
        state_2 = sample(["edited", "deleted"], counts=[2, 1], k=3)

        temp_events = [{"form_id": EventGenerator.generate_key(), "form_title": "This is hard-coded"}
                       for i in range(num_event_keys)]
        self.events = []

        created_events = []
        events_changes = []
        last_changes = []
        deleted_events = []
        for event in temp_events:
            # Store a newly created event
            created_events.append(self.create_event(
                event, happened_at_datetime=happened_at))
        for event in created_events:
            # Store a an event as edited or published
            for _ in range(randint(1, 5)):
                events_changes.append(
                    self.change_event_state(event, state_1))
        for event in events_changes:
            # Store a an event as edited or deleted
            if event["form_id"] not in deleted_events:
                for _ in range(randint(0, 1)):
                    last_changes.append(
                        self.change_event_state(event, state_2))
                    deleted_events.append(event["form_id"])
        self.events.extend(created_events)
        self.events.extend(events_changes)
        self.events.extend(last_changes)
        # shuffle(self.events)
        self.save_to_json()

    @classmethod
    def generate_key(cls, base_num: int = 3):
        """_summary_

        Args:
            base_num (int, optional): the length of the generated key. Defaults to 3.

        Returns:
            str: Returns a randomly generated string key
        """
        str_length = randint(base_num, base_num+3)
        return ''.join(choice(string.ascii_letters).lower() for x in range(str_length))

    def create_event(self, event, event_state: str = "created", happened_at_datetime: datetime = datetime.now()) -> dict:
        """Creates an event with a state

        Args:
            event (dict): An event action containing a state and an event key
            event_state (str, optional): A state of an event. Defaults to "created".

        Returns:
            dict: An event
        """
        new_event = {}
        rand_minutes = randint(3, 10)
        rand_hours = randint(1, 24)
        rand_seconds = randint(1, 60)

        random_added_time = timedelta(minutes=rand_minutes)

        event_happened_at_str = event.get("event_happened_at")

        if event_happened_at_str and event_state != "created":
            random_added_time = timedelta(
                days=randint(0, 23), minutes=rand_minutes, hours=rand_hours, seconds=rand_seconds)
            happened_at_datetime = datetime.strptime(
                event_happened_at_str, '%Y-%m-%d%H:%M:%S')

        event_happened_at = happened_at_datetime + random_added_time
        event_happened_at_str = event_happened_at.strftime(
            "%Y-%m-%d%H:%M:%S")

        event_update = {"event_happened_at": event_happened_at_str,
                        "event_type": event_state}
        new_event = {**event}
        new_event.update(event_update)
        return new_event

    def change_event_state(self, event: dict, states: list):
        """Changes the event state

        Args:
            event (dict): An event action containing a state and an event key
        """
        temp_states = states
        shuffle(temp_states)
        new_state = choice(temp_states)
        # Initialize empty container for events

        # Append event states into container
        changed_event = self.create_event(
            event, event_state=new_state)
        return changed_event

    def save_to_json(self, filename: str = "events.json"):
        """Saves the generated events into a JSON file

        Args:
            filename (str, optional): The name of the file to be saved into.
            Defaults to "events.json".
        """
        if self.events:
            with open(filename, 'w', encoding="utf8") as file_obj:
                file_obj.write(json.dumps(self.events))


def create_producer(config_dict: dict,
                    service: str = LOCAL_KAFKA_SERVICE) -> KafkaProducer | Producer:
    """Creates a Producer that produces to topic[s]

    Args:
        config_dict (dict): A config in a dictionary state
        service (str, optional): he service to use when producing to a topic.
            Defaults to LOCAL_KAFKA_SERVICE.

    Returns:
        Any[KafkaProducer, Producer]: A Kafka Producer
    """

    config_prod = config_dict["producer"]
    producer = None
    if service == CONFLUENT_KAFKA_SERVICE:
        producer = Producer(**config_prod)
    if service == LOCAL_KAFKA_SERVICE:
        producer = KafkaProducer(**config_prod)
    return producer


def create_consumer(topic: str | list[str],  config_dict: dict,
                    service: str = LOCAL_KAFKA_SERVICE) -> KafkaConsumer | Consumer:
    """Creates a consumer subscribed to topic[s]

    Args:
        topic (Any[str, list[str]]): A list of string topucs
        config_dict (dict): A config in a dictionary state
        service (str, optional): he service to use when consuming a topic.
            Defaults to LOCAL_KAFKA_SERVICE.

    Returns:
        Any[KafkaConsumer, Consumer]: A Kafka Consumer

    """
    consumer = None
    if service == LOCAL_KAFKA_SERVICE:
        # Deserialize to JSON
        consumer = KafkaConsumer(
            **config_dict["consumer"],
            value_deserializer=lambda msg: json.loads(msg.decode('utf8')))
        consumer.subscribe(topics=topic)
    if service == CONFLUENT_KAFKA_SERVICE:
        consumer = Consumer(**config_dict["consumer"])
        consumer.subscribe(topic=topic)
    return consumer


def random_country():
    """Generate a random country

    Returns:
        str: a random country language
    """
    countries = ["af", "ax", "al", "dz", "as", "ad", "ao", "ai", "aq", "ag", "ar",
                 "am", "aw", "au", "at", "az", "bs", "bh", "bd", "bb", "by", "be",
                 "bz", "bj", "bm", "bt", "bo", "bq", "ba", "bw", "bv", "br", "io",
                 "bn", "bg", "bf", "bi", "cv", "kh", "cm", "ca", "ky", "cf", "td",
                 "cl", "cn", "cx", "cc", "co", "km", "cg", "cd", "ck", "cr", "ci",
                 "hr", "cu", "cw", "cy", "cz", "dk", "dj", "dm", "do", "ec", "eg",
                 "sv", "gq", "er", "ee", "et", "fk", "fo", "fj", "fi", "fr", "gf",
                 "pf", "tf", "ga", "gm", "ge", "de", "gh", "gi", "gr", "gl", "gd",
                 "gp", "gu", "gt", "gg", "gn", "gw", "gy", "ht", "hm", "va", "hn",
                 "hk", "hu", "is", "in", "id", "ir", "iq", "ie", "im", "il", "it",
                 "jm", "jp", "je", "jo", "kz", "ke", "ki", "kp", "kr", "kw", "kg",
                 "la", "lv", "lb", "ls", "lr", "ly", "li", "lt", "lu", "mo", "mk",
                 "mg", "mw", "my", "mv", "ml", "mt", "mh", "mq", "mr", "mu", "yt",
                 "mx", "fm", "md", "mc", "mn", "me", "ms", "ma", "mz", "mm", "na",
                 "nr", "np", "nl", "nc", "nz", "ni", "ne", "ng", "nu", "nf", "mp",
                 "no", "om", "pk", "pw", "ps", "pa", "pg", "py", "pe", "ph", "pn",
                 "pl", "pt", "pr", "qa", "re", "ro", "ru", "rw", "bl", "sh", "kn",
                 "lc", "mf", "pm", "vc", "ws", "sm", "st", "sa", "sn", "rs", "sc",
                 "sl", "sg", "sx", "sk", "si", "sb", "so", "za", "gs", "ss", "es",
                 "lk", "sd", "sr", "sj", "sz", "se", "ch", "sy", "tw", "tj", "tz",
                 "th", "tl", "tg", "tk", "to", "tt", "tn", "tr", "tm", "tc", "tv",
                 "ug", "ua", "ae", "gb", "us", "um", "uy", "uz", "vu", "ve", "vn",
                 "vg", "vi", "wf", "eh", "ye", "zm", "zw"]
    return choice(countries)


def generate_form_data(form_id: str, form_title: str):
    """Generates form data

    Args:
        form_id (str): the form id
        form_title (str): the form title

    Returns:
        dict: A dictionary with the curent form information
    """
    data = FORM_SAMPLE
    data.update({"id": form_id, "title": form_title, "type": "quiz"})
    language = random_country()
    data["settings"].update({"language": language})
    return data


def convert_event_to_bytes(topic: str, event: dict) -> dict:
    """Converts an event into bytes

    Args:
        topic (str): the topic to publish to
        event (dict): the event

    Returns:
        dict: an event in byte format
    """
    event_bytes = json.dumps(
        event, indent=2).encode('utf-8')
    key = event["form_id"].encode("utf8")
    topic_data = {"topic": topic,
                  "key": key, "value": event_bytes}
    return topic_data


def get_form_data(form_id: str) -> dict:
    """Get the form data as a dict

    Args:
        event (dict): A dictionary that contains an event state

    Returns:
        dict: the form data
    """
    form_title = "This is hard-coded"
    form_data = generate_form_data(form_id, form_title)
    return form_data
