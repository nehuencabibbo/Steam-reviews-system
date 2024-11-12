import os
import logging
from configparser import ConfigParser
from percentile import Percentile
from common.middleware.middleware import Middleware
from common.watchdog_client.watchdog_client import WatchdogClient

def get_config():
    config_params = {}

    config = ConfigParser(os.environ)
    config.read("config.ini")
    try:
        # Node related
        # config_params["NODE_ID"] = os.getenv("NODE_ID")
        config_params["NEEDED_ENDS_TO_FINISH"] = int(os.getenv('NEEDED_ENDS_TO_FINISH', config["DEFAULT"]["NEEDED_ENDS_TO_FINISH"]))

        # queues
        config_params["CONSUME_QUEUE"] = os.getenv("CONSUME_QUEUE", config["DEFAULT"]["CONSUME_QUEUE"])
        config_params["PUBLISH_QUEUE"] = os.getenv("PUBLISH_QUEUE", config["DEFAULT"]["PUBLISH_QUEUE"])
        
        # percentile
        config_params["PERCENTILE"] = int(os.getenv('PERCENTILE', config["DEFAULT"]["PERCENTILE"]))
        
        # storage 
        config_params["RANGE_FOR_PARTITION"] = int(os.getenv('RANGE_FOR_PARTITION', config["DEFAULT"]["RANGE_FOR_PARTITION"]))
        config_params["SAVE_AFTER_MESSAGES"] = int(os.getenv('SAVE_AFTER_MESSAGES', config["DEFAULT"]["SAVE_AFTER_MESSAGES"]))
        config_params["STORAGE_DIR"] = os.getenv('STORAGE_DIR', config["DEFAULT"]["STORAGE_DIR"])

        # logging
        config_params["LOGGING_LEVEL"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        
        # broker ip
        config_params["RABBIT_IP"] = os.getenv('RABBIT_IP', config["DEFAULT"]["RABBIT_IP"])

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
        format='[%(levelname)s]   %(message)s',
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

    monitor_ip = config.pop("WATCHDOG_IP")
    monitor_port = config.pop("WATCHDOG_PORT")
    node_name = config.pop("NODE_NAME")
    monitor = WatchdogClient(monitor_ip, monitor_port, node_name)

    percentile = Percentile(config, middleware, monitor)
    percentile.run()


main()