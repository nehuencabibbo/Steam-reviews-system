import os
from configparser import ConfigParser
import logging

from watchdog import Watchdog
from common.server_socket.tcp_middleware import TCPMiddleware


def get_config():
    config_params = {}
    config = ConfigParser(os.environ)
    config.read("config.ini")
    try:
        config_params["LOGGING_LEVEL"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )

        config_params["NODE_ID"] = int(os.getenv("NODE_ID", config["DEFAULT"]["NODE_ID"]))
        config_params["PORT"] = int(os.getenv("PORT", config["DEFAULT"]["PORT"]))
        config_params["ELECTION_PORT"] = int(os.getenv("ELECTION_PORT", config["DEFAULT"]["ELECTION_PORT"]))
        config_params["LEADER_COMUNICATION_PORT"] = int(os.getenv("LEADER_COMUNICATION_PORT",
                                                        config["DEFAULT"]["LEADER_COMUNICATION_PORT"])
                                                    )
        
        config_params["WAIT_BETWEEN_HEARTBEAT"] = float(
            os.getenv("WAIT_BETWEEN_HEARTBEAT", 
            config["DEFAULT"]["WAIT_BETWEEN_HEARTBEAT"],
            )
        )
        config_params["LEADER_DISCOVERY_PORT"] = int(
            os.getenv("LEADER_DISCOVERY_PORT", 
            config["DEFAULT"]["LEADER_DISCOVERY_PORT"]
            )
        )

        config_params["AMOUNT_OF_MONITORS"] = int(
            os.getenv("AMOUNT_OF_MONITORS", 
            config["DEFAULT"]["AMOUNT_OF_MONITORS"]
            )
        )

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

    config.pop("LOGGING_LEVEL", None)

    middleware = TCPMiddleware()
    watchdog = Watchdog(middleware, config)

    watchdog.start()


if __name__ == "__main__":
    main()

