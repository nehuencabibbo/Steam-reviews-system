# Parent directory is included in the search path for modules
import os

from configparser import ConfigParser
import logging

from watchdog import Watchdog
from common.server_socket.server_socket import ServerSocket


def get_config():
    config_params = {}
    config = ConfigParser(os.environ)
    try:
        config_params["LOGGING_LEVEL"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )

        config_params["NODE_ID"] = os.getenv("NODE_ID", config["DEFAULT"]["NODE_ID"])
        config_params["PORT"] = int(os.getenv("PORT", config["DEFAULT"]["PORT"]))

        config_params["WAIT_BETWEEN_HEARTBEAT"] = float(
            os.getenv("WAIT_BETWEEN_HEARTBEAT", 
            config["DEFAULT"]["WAIT_BETWEEN_HEARTBEAT"],
            )
        )

        #TODO: add env var for lider election nodes

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

    #middleware = Middleware(config["RABBIT_IP"])
    config.pop("LOGGING_LEVEL", None)

    socket = ServerSocket(config["PORT"])
    config.pop("PORT", None)

    watchdog = Watchdog(socket, config)

    watchdog.start()


if __name__ == "__main__":
    main()

