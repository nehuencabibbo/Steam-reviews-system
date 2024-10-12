import os
from client import Client
from configparser import ConfigParser
import logging
from common.middleware.middleware import Middleware
from common.protocol.protocol import Protocol
import time

def get_config():
    config_params = {}

    config = ConfigParser(os.environ)
    config.read("config.ini")
    try:

        # sender queues
        config_params["GAMES_QUEUE"] = os.getenv("GAMES_QUEUE", config["DEFAULT"]["GAMES_QUEUE"])
        config_params["REVIEWS_QUEUE"] = os.getenv("REVIEWS_QUEUE", config["DEFAULT"]["REVIEWS_QUEUE"])

        # queues for query results
        config_params["Q1_RESULT_QUEUE"] = os.getenv("Q1_RESULT_QUEUE", config["DEFAULT"]["Q1_RESULT_QUEUE"])
        config_params["Q2_RESULT_QUEUE"] = os.getenv("Q2_RESULT_QUEUE", config["DEFAULT"]["Q2_RESULT_QUEUE"])
        config_params["Q3_RESULT_QUEUE"] = os.getenv("Q3_RESULT_QUEUE", config["DEFAULT"]["Q3_RESULT_QUEUE"])
        config_params["Q4_RESULT_QUEUE"] = os.getenv("Q4_RESULT_QUEUE", config["DEFAULT"]["Q4_RESULT_QUEUE"])
        config_params["Q5_RESULT_QUEUE"] = os.getenv("Q5_RESULT_QUEUE", config["DEFAULT"]["Q5_RESULT_QUEUE"])
        
        # file path
        config_params["GAME_FILE_PATH"] = os.getenv("GAME_FILE_PATH", config["DEFAULT"]["GAME_FILE_PATH"])
        config_params["REVIEWS_FILE_PATH"] = os.getenv("REVIEWS_FILE_PATH", config["DEFAULT"]["REVIEWS_FILE_PATH"])
        
        config_params["LOGGING_LEVEL"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["SENDING_WAIT_TIME"] = int(os.getenv('SENDING_WAIT_TIME', config["DEFAULT"]["SENDING_WAIT_TIME"]))

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

    client = Client(config, middleware, protocol)
    client.run()


start_time = time.time()
main()
print(f"--- {round(time.time() - start_time, 2)} seconds ---")