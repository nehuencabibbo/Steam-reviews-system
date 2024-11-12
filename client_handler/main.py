import os
from client_handler import ClientHandler
from common.client_middleware.client_middleware import ClientMiddleware
from configparser import ConfigParser
import logging
from common.middleware.middleware import Middleware
from common.protocol.protocol import Protocol
from common.watchdog_client.watchdog_client import WatchdogClient

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

        # Clients port
        config_params["CLIENTS_PORT"] = os.getenv(
            "CLIENTS_PORT", config["DEFAULT"]["CLIENTS_PORT"]
        )

        # queues

        # queues for query results
        config_params["Q1_RESULT_QUEUE"] = os.getenv(
            "Q1_RESULT_QUEUE", config["DEFAULT"]["Q1_RESULT_QUEUE"]
        )
        config_params["Q2_RESULT_QUEUE"] = os.getenv(
            "Q2_RESULT_QUEUE", config["DEFAULT"]["Q2_RESULT_QUEUE"]
        )
        config_params["Q3_RESULT_QUEUE"] = os.getenv(
            "Q3_RESULT_QUEUE", config["DEFAULT"]["Q3_RESULT_QUEUE"]
        )
        config_params["Q4_RESULT_QUEUE"] = os.getenv(
            "Q4_RESULT_QUEUE", config["DEFAULT"]["Q4_RESULT_QUEUE"]
        )
        config_params["Q5_RESULT_QUEUE"] = os.getenv(
            "Q5_RESULT_QUEUE", config["DEFAULT"]["Q5_RESULT_QUEUE"]
        )

        # exchanges
        config_params["GAMES_QUEUE_NAME"] = os.getenv(
            "GAMES_QUEUE_NAME",
            config["DEFAULT"]["GAMES_QUEUE_NAME"],
        )

        config_params["REVIEWS_QUEUE_NAME"] = os.getenv(
            "REVIEWS_QUEUE_NAME",
            config["DEFAULT"]["REVIEWS_QUEUE_NAME"],
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

    logging_level = config.pop("LOGGING_LEVEL")
    init_logger(logging_level)
    logging.debug("Logging configuration:")
    [logging.debug(f"{key}: {value}") for key, value in config.items()]

    broker_ip = config["RABBIT_IP"]
    middleware = Middleware(broker_ip)
    client_middleware = ClientMiddleware()
    protocol = Protocol()
    
    monitor_ip = config.pop("WATCHDOG_IP")
    monitor_port = config.pop("WATCHDOG_PORT")
    node_name = config.pop("NODE_NAME")
    monitor = WatchdogClient(monitor_ip, monitor_port, node_name)

    counter = ClientHandler(
        client_middleware=client_middleware,
        middleware=middleware,
        protocol=protocol,
        client_monitor=monitor,
        **config,
    )

    logging.info("RUNNING CLIENT HANDLER")
    counter.run()


main()
