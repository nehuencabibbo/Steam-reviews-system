import os
from counter_by_app_id import CounterByAppId
from configparser import ConfigParser
import logging
from common.middleware.middleware import Middleware
from common.protocol.protocol import Protocol
def get_config():
    config_params = {}

    config = ConfigParser(os.environ)
    config.read("config.ini")
    try:
        # ID
        config_params["NODE_ID"] = int(os.getenv("NODE_ID"))

        # queues
        config_params["CONSUME_QUEUE_SUFIX"] = os.getenv("CONSUME_QUEUE_SUFIX", config["DEFAULT"]["CONSUME_QUEUE_SUFIX"])
        config_params["PUBLISH_QUEUE"] = os.getenv("PUBLISH_QUEUE", config["DEFAULT"]["PUBLISH_QUEUE"])

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
    
    counter = CounterByAppId(config, middleware, protocol)
    logging.info("RUNNING COUNTER")
    counter.run()


main()