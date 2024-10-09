# Parent directory is included in the search path for modules
import os

from common.middleware.middleware import Middleware
from common.protocol.protocol import Protocol

from configparser import ConfigParser
import logging

from top_k.top_k import TopK


def get_config():
    config_params = {}
    print("CWD: ", os.getcwd())
    config = ConfigParser(os.environ)
    config.read("./top_k/config.ini")
    try:
        config_params["LOGGING_LEVEL"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
        config_params["RABBIT_IP"] = os.getenv(
            "RABBIT_IP", config["DEFAULT"]["RABBIT_IP"]
        )

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

    top_k = TopK(protocol, middleware, config)
    top_k.start()


if __name__ == "__main__":
    main()
