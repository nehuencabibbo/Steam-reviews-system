# Parent directory is included in the search path for modules
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.middleware.middleware import Middleware
from common.protocol.protocol import Protocol
from filter_by_column_value import FilterColumnByValue
from common.watchdog_client.watchdog_client import WatchdogClient

from configparser import ConfigParser
import logging


def get_config():
    config_params = {}

    config = ConfigParser(os.environ)
    config.read("config.ini")
    try:
        # General config
        config_params["LOGGING_LEVEL"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
        config_params["RABBIT_IP"] = os.getenv(
            "RABBIT_IP", config["DEFAULT"]["RABBIT_IP"]
        )
        config_params["INSTANCES_OF_MYSELF"] = os.getenv(
            "INSTANCES_OF_MYSELF", config["DEFAULT"]["INSTANCES_OF_MYSELF"]
        )

        # Node related
        config_params["NODE_ID"] = os.getenv("NODE_ID", config["DEFAULT"]["NODE_ID"])
        config_params["COLUMN_NUMBER_TO_USE"] = int(
            os.getenv("COLUMN_NUMBER_TO_USE", config["DEFAULT"]["COLUMN_NUMBER_TO_USE"])
        )
        config_params["VALUE_TO_FILTER_BY"] = os.getenv(
            "VALUE_TO_FILTER_BY", config["DEFAULT"]["VALUE_TO_FILTER_BY"]
        )
        config_params["CRITERIA"] = os.getenv("CRITERIA", config["DEFAULT"]["CRITERIA"])

        columns_to_keep = os.getenv(
            "COLUMNS_TO_KEEP", config["DEFAULT"]["COLUMNS_TO_KEEP"]
        ).split(",")
        columns_to_keep = [int(column) for column in columns_to_keep]
        config_params["COLUMNS_TO_KEEP"] = columns_to_keep

        # Reciving queues
        config_params["RECIVING_QUEUE_NAME"] = os.getenv(
            "RECIVING_QUEUE_NAME", config["DEFAULT"]["RECIVING_QUEUE_NAME"]
        )

        # Forwarding queues
        # This is a list of forwarding queues
        forwarding_queues_env_var = os.getenv(
                "FORWARDING_QUEUE_NAMES",
                config["DEFAULT"]["FORWARDING_QUEUE_NAMES"],
            )
        config_params["FORWARDING_QUEUE_NAMES"] = forwarding_queues_env_var.split(',')
        # This is a list of amount_of_forwarding queues, where
        # the element 0 corresponds to the amount of forwarding queues in
        # the first position of config_params["FORWARDING_QUEUE_NAME"],
        # element 1 corresponds to the second position of the list, and
        # so on
        amount_of_forwarding_queues_env_var = os.getenv(
                "AMOUNT_OF_FORWARDING_QUEUES",
                config["DEFAULT"]["AMOUNT_OF_FORWARDING_QUEUES"],
            )
        amount_of_forwarding_queues = [
            int(value) for value in amount_of_forwarding_queues_env_var.split(',')
        ]
        config_params["AMOUNT_OF_FORWARDING_QUEUES"] = amount_of_forwarding_queues

        config_params["BATCH_SIZE"] = int(
            os.getenv(
                "BATCH_SIZE",
                config["DEFAULT"]["BATCH_SIZE"],
            )
        )

        config_params["PREFETCH_COUNT"] = int(
            os.getenv(
                "PREFETCH_COUNT",
                config["DEFAULT"]["PREFETCH_COUNT"],
            )
        )

        # # Monitor
        config_params["WATCHDOG_IP"] = os.getenv("WATCHDOG_IP")

        config_params["WATCHDOG_PORT"] = int(os.getenv("WATCHDOG_PORT"))

        config_params["NODE_NAME"] = os.getenv("NODE_NAME")

        # TODO: Raise an error if __REQUIRED__ is paresed anywhere here
    except KeyError as e:
        raise KeyError(f"Key was not found. Error: {e}. Aborting")
    except ValueError as e:
        raise ValueError(f"Key could not be parsed. Error: {e}. Aborting")

    return config_params


def init_logger(logging_level):
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(
        format="[%(levelname)s]   %(message)s",
        level=logging_level,
    )


def main():
    config = get_config()
    init_logger(config["LOGGING_LEVEL"])
    logging.debug("Logging configuration:")
    [logging.debug(f"{key}: {value}") for key, value in config.items()]

    protocol = Protocol()
    middleware = Middleware(
        config["RABBIT_IP"],
        prefetch_count=config["PREFETCH_COUNT"],
        batch_size=config["BATCH_SIZE"],
    )
    config.pop("RABBIT_IP", None)
    config.pop("LOGGING_LEVEL", None)

    monitor_ip = config.pop("WATCHDOG_IP")
    monitor_port = config.pop("WATCHDOG_PORT")
    node_name = config.pop("NODE_NAME")
    monitor = WatchdogClient(monitor_ip, monitor_port, node_name)

    filter_column_by_value = FilterColumnByValue(protocol, middleware, monitor, config)
    filter_column_by_value.start()


main()
