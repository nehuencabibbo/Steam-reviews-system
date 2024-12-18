# Parent directory is included in the search path for modules
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.middleware.middleware import Middleware
from filter_columns import FilterColumns
from common.protocol.protocol import Protocol
from common.watchdog_client.watchdog_client import WatchdogClient

from configparser import ConfigParser
import logging


def get_config():
    config_params = {}

    config = ConfigParser(os.environ)
    config.read("config.ini")
    try:
        # Node config
        config_params["NODE_ID"] = os.getenv("NODE_ID", config["DEFAULT"]["NODE_ID"])
        config_params["INSTANCES_OF_MYSELF"] = os.getenv(
            "INSTANCES_OF_MYSELF", config["DEFAULT"]["INSTANCES_OF_MYSELF"]
        )

        # Reciving queues
        config_params["CLIENT_GAMES_QUEUE_NAME"] = os.getenv(
            "CLIENT_GAMES_QUEUE_NAME", config["DEFAULT"]["CLIENT_GAMES_QUEUE_NAME"]
        )
        config_params["CLIENT_REVIEWS_QUEUE_NAME"] = os.getenv(
            "CLIENT_REVIEWS_QUEUE_NAME", config["DEFAULT"]["CLIENT_REVIEWS_QUEUE_NAME"]
        )

        # Forwarding queues
        config_params["NULL_DROP_GAMES_QUEUE_NAME"] = os.getenv(
            "NULL_DROP_GAMES_QUEUE_NAME",
            config["DEFAULT"]["NULL_DROP_GAMES_QUEUE_NAME"],
        )
        config_params["NULL_DROP_REVIEWS_QUEUE_NAME"] = os.getenv(
            "NULL_DROP_REVIEWS_QUEUE_NAME",
            config["DEFAULT"]["NULL_DROP_REVIEWS_QUEUE_NAME"],
        )

        # Dataset related
        games_columns_to_keep = os.getenv(
            "GAMES_COLUMNS_TO_KEEP", config["DEFAULT"]["GAMES_COLUMNS_TO_KEEP"]
        ).split(",")
        games_columns_to_keep = [int(column) for column in games_columns_to_keep]
        config_params["GAMES_COLUMNS_TO_KEEP"] = games_columns_to_keep

        reviews_columns_to_keep = os.getenv(
            "REVIEWS_COLUMNS_TO_KEEP", config["DEFAULT"]["REVIEWS_COLUMNS_TO_KEEP"]
        ).split(",")
        reviews_columns_to_keep = [int(column) for column in reviews_columns_to_keep]
        config_params["REVIEWS_COLUMNS_TO_KEEP"] = reviews_columns_to_keep

        # General config
        config_params["LOGGING_LEVEL"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
        config_params["RABBIT_IP"] = os.getenv(
            "RABBIT_IP", config["DEFAULT"]["RABBIT_IP"]
        )


        # # Monitor
        config_params["WATCHDOGS_IP"] = os.getenv("WATCHDOGS_IP").split(",")

        config_params["WATCHDOG_PORT"] = int(os.getenv("WATCHDOG_PORT"))

        config_params["NODE_NAME"] = os.getenv("NODE_NAME")

        config_params["LEADER_DISCOVERY_PORT"] = int(os.getenv("LEADER_DISCOVERY_PORT"))

        #Testing
        config_params["EXIT"] = int(os.getenv("EXIT", config["DEFAULT"]["EXIT"]))
        
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

    middleware = Middleware(config["RABBIT_IP"], use_logging=True)
    config.pop("RABBIT_IP", None)
    config.pop("LOGGING_LEVEL", None)

    monitor_ip = config.pop("WATCHDOGS_IP")
    monitor_port = config.pop("WATCHDOG_PORT")
    node_name = config.pop("NODE_NAME")
    discovery_port = config.pop("LEADER_DISCOVERY_PORT")
    monitor = WatchdogClient(monitor_ip, monitor_port, node_name, discovery_port, middleware)

    filter_columns = FilterColumns(middleware, monitor, config)

    filter_columns.start()


main()
