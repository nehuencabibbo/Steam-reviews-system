import os
from client_handler import ClientHandler
from common.client_middleware.client_middleware import ClientMiddleware
from configparser import ConfigParser
import logging
from common.middleware.middleware import Middleware
from common.protocol.protocol import Protocol


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

        # exchanges
        config_params["NEW_CLIENTS_EXCHANGE_NAME"] = os.getenv(
            "NEW_CLIENTS_EXCHANGE_NAME",
            config["DEFAULT"]["NEW_CLIENTS_EXCHANGE_NAME"],
        )

        # # Forwarding queue
        # config_params["FORWARDING_QUEUE_NAME"] = os.getenv(
        #     "FORWARDING_QUEUE_NAME",
        #     config["DEFAULT"]["FORWARDING_QUEUE_NAME"],
        # )

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

    broker_ip = config.pop("RABBIT_IP")
    middleware = Middleware(broker_ip)
    client_middleware = ClientMiddleware()
    protocol = Protocol()

    counter = ClientHandler(
        client_middleware=client_middleware,
        middleware=middleware,
        protocol=protocol,
        **config,
    )

    logging.info("RUNNING CLIENT HANDLER")
    counter.run()


main()
