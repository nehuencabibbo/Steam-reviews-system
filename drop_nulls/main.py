# Parent directory is included in the search path for modules
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from middleware.middleware import Middleware
from common.protocol.protocol import Protocol
from drop_nulls import DropNulls

from configparser import ConfigParser
import logging


def get_config():
    config_params = {}

    config = ConfigParser(os.environ)
    config.read("config.ini")
    try:
        # General config
        config_params["NODE_ID"] = os.getenv("NODE_ID", config["DEFAULT"]["NODE_ID"])
        config_params["LOGGING_LEVEL"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
        config_params["RABBIT_IP"] = os.getenv(
            "RABBIT_IP", config["DEFAULT"]["RABBIT_IP"]
        )
        config_params["COUNT_BY_PLATFORM_NODES"] = int(
            os.getenv(
                "COUNT_BY_PLATFORM_NODES", config["DEFAULT"]["COUNT_BY_PLATFORM_NODES"]
            )
        )
        config_params["INSTANCES_OF_MYSELF"] = os.getenv("INSTANCES_OF_MYSELF", config["DEFAULT"]["INSTANCES_OF_MYSELF"])

        # Reciving queues
        config_params["GAMES_RECIVING_QUEUE_NAME"] = os.getenv(
            "GAMES_RECIVING_QUEUE_NAME", config["DEFAULT"]["GAMES_RECIVING_QUEUE_NAME"]
        )
        config_params["REVIEWS_RECIVING_QUEUE_NAME"] = os.getenv(
            "REVIEWS_RECIVING_QUEUE_NAME",
            config["DEFAULT"]["REVIEWS_RECIVING_QUEUE_NAME"],
        )

        # Forwarding queues
        # Q1
        config_params["Q1_PLATFORM"] = os.getenv(
            "Q1_PLATFORM", config["DEFAULT"]["Q1_PLATFORM"]
        )
        # Q2
        config_params["Q2_GAMES"] = os.getenv("Q2_GAMES", config["DEFAULT"]["Q2_GAMES"])
        config_params["Q2_FORWARD_NODES"] = int(
            os.getenv("Q2_FORWARD_NODES", config["DEFAULT"]["Q2_FORWARD_NODES"])
        )
        # Q3
        config_params["Q3_GAMES"] = os.getenv("Q3_GAMES", config["DEFAULT"]["Q3_GAMES"])
        config_params["Q3_REVIEWS"] = os.getenv(
            "Q3_REVIEWS", config["DEFAULT"]["Q3_REVIEWS"]
        )
        config_params["Q3_FORWARD_NODES"] = int(
            os.getenv("Q3_FORWARD_NODES", config["DEFAULT"]["Q3_FORWARD_NODES"])
        )
        # Q4
        config_params["Q4_GAMES"] = os.getenv("Q4_GAMES", config["DEFAULT"]["Q4_GAMES"])
        config_params["Q4_REVIEWS"] = os.getenv(
            "Q4_REVIEWS", config["DEFAULT"]["Q4_REVIEWS"]
        )
        config_params["Q4_FORWARD_NODES"] = int(
            os.getenv("Q4_FORWARD_NODES", config["DEFAULT"]["Q4_FORWARD_NODES"])
        )
        # Q5
        config_params["Q5_GAMES"] = os.getenv("Q5_GAMES", config["DEFAULT"]["Q5_GAMES"])
        config_params["Q5_REVIEWS"] = os.getenv(
            "Q5_REVIEWS", config["DEFAULT"]["Q5_REVIEWS"]
        )
        config_params["Q5_FORWARD_NODES"] = int(
            os.getenv("Q5_FORWARD_NODES", config["DEFAULT"]["Q5_FORWARD_NODES"])
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

    protocol = Protocol()
    middleware = Middleware(config["RABBIT_IP"])
    config.pop("RABBIT_IP", None)
    config.pop("LOGGING_LEVEL", None)

    drop_nulls = DropNulls(protocol, middleware, config)
    drop_nulls.start()


main()
