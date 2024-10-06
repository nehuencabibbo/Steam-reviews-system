# Parent directory is included in the search path for modules
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from middleware.middleware import Middleware
from common.protocol.protocol import Protocol
from filter_by_column_value import FilterColumnByValue

from configparser import ConfigParser
import logging

def get_config():
    config_params = {}

    config = ConfigParser(os.environ)
    config.read("config.ini")
    try:
        # General config 
        config_params["LOGGING_LEVEL"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["RABBIT_IP"] = os.getenv('RABBIT_IP', config["DEFAULT"]["RABBIT_IP"])

        # Node related
        config_params["NODE_ID"] = os.getenv("NODE_ID", config["DEFAULT"]["NODE_ID"])
        config_params["COLUMN_NUMBER_TO_USE"] = int(os.getenv("COLUMN_NUMBER_TO_USE", config["DEFAULT"]["COLUMN_NUMBER_TO_USE"]))
        config_params["VALUE_TO_FILTER_BY"] = os.getenv("VALUE_TO_FILTER_BY", config["DEFAULT"]["VALUE_TO_FILTER_BY"])
        config_params["CRITERIA"] = os.getenv("CRITERIA", config["DEFAULT"]["CRITERIA"])

        columns_to_keep = os.getenv("COLUMNS_TO_KEEP", config["DEFAULT"]["COLUMNS_TO_KEEP"]).split(',')
        columns_to_keep = [int(column) for column in columns_to_keep]
        config_params["COLUMNS_TO_KEEP"] = columns_to_keep
        
        # Reciving queues 
        config_params["RECIVING_QUEUE_NAME"] = os.getenv("RECIVING_QUEUE_NAME", config["DEFAULT"]["RECIVING_QUEUE_NAME"])

        # Forwarding queues 
        config_params["FORWARDING_QUEUE_NAME"] = os.getenv('FORWARDING_QUEUE_NAME', config["DEFAULT"]["FORWARDING_QUEUE_NAME"])
        config_params["AMOUNT_OF_FORWARDING_QUEUES"] = int(os.getenv('AMOUNT_OF_FORWARDING_QUEUES', config["DEFAULT"]["AMOUNT_OF_FORWARDING_QUEUES"]))

        # TODO: Raise an error if __REQUIRED__ is paresed anywhere here 
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
    init_logger(config["LOGGING_LEVEL"])
    logging.debug("Logging configuration:")
    [logging.debug(f"{key}: {value}") for key, value in config.items()]

    protocol = Protocol()
    middleware = Middleware(config["RABBIT_IP"])
    config.pop("RABBIT_IP", None)
    config.pop("LOGGING_LEVEL", None)

    filter_column_by_value = FilterColumnByValue(protocol, middleware, config)
    filter_column_by_value.start()


main()