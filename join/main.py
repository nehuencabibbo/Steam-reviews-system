# Parent directory is included in the search path for modules
import os
from time import sleep

from join.join import Join
from common.middleware.middleware import Middleware
from common.protocol.protocol import Protocol
from common.activity_log.activity_log import ActivityLog

from configparser import ConfigParser
import logging


def get_config():
    config_params = {}
    config = ConfigParser(os.environ)
    config.read("./join/config.ini")
    try:
        # General config
        config_params["LOGGING_LEVEL"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
        config_params["RABBIT_IP"] = os.getenv(
            "RABBIT_IP", config["DEFAULT"]["RABBIT_IP"]
        )
        config_params["NODE_ID"] = os.getenv("NODE_ID", config["DEFAULT"]["NODE_ID"])

        # Reciving queues
        config_params["INPUT_GAMES_QUEUE_NAME"] = os.getenv(
            "INPUT_GAMES_QUEUE_NAME", config["DEFAULT"]["INPUT_GAMES_QUEUE_NAME"]
        )
        config_params["INPUT_REVIEWS_QUEUE_NAME"] = os.getenv(
            "INPUT_REVIEWS_QUEUE_NAME", config["DEFAULT"]["INPUT_REVIEWS_QUEUE_NAME"]
        )

        # Forwarding queues
        config_params["OUTPUT_QUEUE_NAME"] = os.getenv(
            "OUTPUT_QUEUE_NAME", config["DEFAULT"]["OUTPUT_QUEUE_NAME"]
        )
        config_params["AMOUNT_OF_FORWARDING_QUEUES"] = int(
            os.getenv(
                "AMOUNT_OF_FORWARDING_QUEUES",
                config["DEFAULT"]["AMOUNT_OF_FORWARDING_QUEUES"],
            )
        )

        # Functionality
        config_params["PARTITION_RANGE"] = os.getenv(
            "PARTITION_RANGE", config["DEFAULT"]["PARTITION_RANGE"]
        )
        config_params["NEEDED_REVIEWS_ENDS"] = int(
            os.getenv("NEEDED_REVIEWS_ENDS", config["DEFAULT"]["NEEDED_REVIEWS_ENDS"])
        )
        config_params["NEEDED_GAMES_ENDS"] = int(
            os.getenv("NEEDED_GAMES_ENDS", config["DEFAULT"]["NEEDED_GAMES_ENDS"])
        )

        # For forwarding to the client 
        config_params["INSTANCES_OF_MYSELF"] = int(
            os.getenv("INSTANCES_OF_MYSELF", config["DEFAULT"]["INSTANCES_OF_MYSELF"])
        )

        # Games columns to keep
        games_columns_to_keep = os.getenv(
            "GAMES_COLUMNS_TO_KEEP", config["DEFAULT"]["GAMES_COLUMNS_TO_KEEP"]
        ).split(",")
        games_columns_to_keep = (
            [int(column) for column in games_columns_to_keep]
            if games_columns_to_keep[0] != ""
            else []
        )
        config_params["GAMES_COLUMNS_TO_KEEP"] = games_columns_to_keep

        # Reviews columns to keep
        reviews_columns_to_keep = os.getenv(
            "REVIEWS_COLUMNS_TO_KEEP", config["DEFAULT"]["REVIEWS_COLUMNS_TO_KEEP"]
        ).split(",")
        reviews_columns_to_keep = (
            [int(column) for column in reviews_columns_to_keep]
            if reviews_columns_to_keep[0] != ""
            else []
        )
        config_params["REVIEWS_COLUMNS_TO_KEEP"] = reviews_columns_to_keep

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
    [logging.info(f"{key}: {value}") for key, value in config.items()]

    middleware = Middleware(config["RABBIT_IP"])
    config.pop("RABBIT_IP", None)
    config.pop("LOGGING_LEVEL", None)

    activity_log = ActivityLog()

    join = Join(middleware, config, activity_log)
    join.start()


if __name__ == "__main__":
    main()
