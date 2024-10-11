# Parent directory is included in the search path for modules
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.middleware.middleware import Middleware
from filter_columns import FilterColumns
from common.protocol.protocol import Protocol

from configparser import ConfigParser
import logging


def get_config():
    config_params = {}

    config = ConfigParser(os.environ)
    config.read("config.ini")
    try:
        # Node config 
        config_params["NODE_ID"] = os.getenv("NODE_ID", config["DEFAULT"]["NODE_ID"])
        config_params["INSTANCES_OF_MYSELF"] = int(os.getenv("INSTANCES_OF_MYSELF", config["DEFAULT"]["INSTANCES_OF_MYSELF"]))

        # Reciving queue
        config_params["RECIVING_QUEUE_NAME"] = os.getenv(
            "RECIVING_QUEUE_NAME", config["DEFAULT"]["RECIVING_QUEUE_NAME"]
        )

        # Forwarding queue
        config_params["FORWARDING_QUEUE_NAME"] = os.getenv(
            "FORWARDING_QUEUE_NAME", config["DEFAULT"]["FORWARDING_QUEUE_NAME"]
        )

        # Dataset related 
        columns_to_keep =  os.getenv(
            "COLUMNS_TO_KEEP", config["DEFAULT"]["COLUMNS_TO_KEEP"]
        ).split(",")
        columns_to_keep = [int(column) for column in columns_to_keep]
        config_params["COLUMNS_TO_KEEP"] = columns_to_keep

        # General config 
        config_params["LOGGING_LEVEL"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
        config_params["RABBIT_IP"] = os.getenv(
            "RABBIT_IP", config["DEFAULT"]["RABBIT_IP"]
        )

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

    middleware = Middleware(config["RABBIT_IP"])
    config.pop("RABBIT_IP", None)
    config.pop("LOGGING_LEVEL", None)

    protocol = Protocol()

    filter_columns = FilterColumns(protocol, middleware, config)
    filter_columns.start()


main()
