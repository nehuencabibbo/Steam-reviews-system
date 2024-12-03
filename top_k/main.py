# Parent directory is included in the search path for modules
import os
from common.activity_log.activity_log import ActivityLog

from configparser import ConfigParser
import logging

from common.middleware.middleware import Middleware

from top_k.top_k import TopK
from common.watchdog_client.watchdog_client import WatchdogClient


def get_config():
    config_params = {}
    config = ConfigParser(os.environ)
    config.read("./top_k/config.ini")
    try:
        config_params["LOGGING_LEVEL"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
        config_params["RABBIT_IP"] = os.getenv(
            "RABBIT_IP", config["DEFAULT"]["RABBIT_IP"]
        )

        config_params["NODE_ID"] = os.getenv("NODE_ID", config["DEFAULT"]["NODE_ID"])

        config_params["K"] = os.getenv("K", config["DEFAULT"]["K"])

        config_params["INPUT_TOP_K_QUEUE_NAME"] = os.getenv(
            "INPUT_TOP_K_QUEUE_NAME", config["DEFAULT"]["INPUT_TOP_K_QUEUE_NAME"]
        )
        config_params["OUTPUT_TOP_K_QUEUE_NAME"] = os.getenv(
            "OUTPUT_TOP_K_QUEUE_NAME", config["DEFAULT"]["OUTPUT_TOP_K_QUEUE_NAME"]
        )

        config_params["PARTITION_RANGE"] = os.getenv(
            "PARTITION_RANGE", config["DEFAULT"]["PARTITION_RANGE"]
        )

        config_params["AMOUNT_OF_RECEIVING_QUEUES"] = int(
            os.getenv(
                "AMOUNT_OF_RECEIVING_QUEUES",
                config["DEFAULT"]["AMOUNT_OF_RECEIVING_QUEUES"],
            )
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

    middleware = Middleware(config["RABBIT_IP"])
    config.pop("RABBIT_IP", None)
    config.pop("LOGGING_LEVEL", None)


    monitor_ip = config.pop("WATCHDOGS_IP")
    monitor_port = config.pop("WATCHDOG_PORT")
    node_name = config.pop("NODE_NAME")
    discovery_port = config.pop("LEADER_DISCOVERY_PORT")
    monitor = WatchdogClient(monitor_ip, monitor_port, node_name, discovery_port, middleware)
    activity_log = ActivityLog()

    top_k = TopK(middleware, monitor, config, activity_log)

    top_k.start()


if __name__ == "__main__":
    main()
