# Parent directory is included in the search path for modules
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.middleware.middleware import Middleware
from common.protocol.protocol import Protocol
from drop_nulls import DropNulls
from common.watchdog_client.watchdog_client import WatchdogClient

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
        config_params["INSTANCES_OF_MYSELF"] = os.getenv(
            "INSTANCES_OF_MYSELF", config["DEFAULT"]["INSTANCES_OF_MYSELF"]
        )

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

        # Q3
        config_params["Q3_GAMES"] = os.getenv("Q3_GAMES", config["DEFAULT"]["Q3_GAMES"])
        config_params["Q3_REVIEWS"] = os.getenv(
            "Q3_REVIEWS", config["DEFAULT"]["Q3_REVIEWS"]
        )

        # Q4
        config_params["Q4_GAMES"] = os.getenv("Q4_GAMES", config["DEFAULT"]["Q4_GAMES"])
        config_params["Q4_REVIEWS"] = os.getenv(
            "Q4_REVIEWS", config["DEFAULT"]["Q4_REVIEWS"]
        )

        # Q5
        config_params["Q5_GAMES"] = os.getenv("Q5_GAMES", config["DEFAULT"]["Q5_GAMES"])
        config_params["Q5_REVIEWS"] = os.getenv(
            "Q5_REVIEWS", config["DEFAULT"]["Q5_REVIEWS"]
        )

        # # Monitor
        config_params["WATCHDOG_IP"] = os.getenv("WATCHDOG_IP")

        config_params["WATCHDOG_PORT"] = int(os.getenv("WATCHDOG_PORT"))

        config_params["NODE_NAME"] = os.getenv("NODE_NAME")

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

    monitor_ip = config.pop("WATCHDOG_IP")
    monitor_port = config.pop("WATCHDOG_PORT")
    node_name = config.pop("NODE_NAME")
    monitor = WatchdogClient(monitor_ip, monitor_port, node_name, middleware)

    drop_nulls = DropNulls(protocol, middleware, monitor, config)
    drop_nulls.start()


main()
