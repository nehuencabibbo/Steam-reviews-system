import os
from percentile import Percentile
from configparser import ConfigParser
import logging
from common.middleware.middleware import Middleware
from common.protocol.protocol import Protocol

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
    
    broker_ip = config.pop("RABBIT_IP")
    middleware = Middleware(broker_ip)
    protocol = Protocol()

    percentile = Percentile(config, middleware, protocol)
    percentile.run()


main()