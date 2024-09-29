import os
from client import Client
from configparser import ConfigParser
import logging

def get_config():
    config_params = {}

    config = ConfigParser(os.environ)
    config.read("config.ini")
    try:
        config_params["CLIENT_ID"] = os.getenv("CLI_ID")

        config_params["GAMES_QUEUE"] = os.getenv("GAMES_QUEUE", config["DEFAULT"]["GAMES_QUEUE"])
        config_params["REVIEWS_QUEUE"] = os.getenv("REVIEWS_QUEUE", config["DEFAULT"]["REVIEWS_QUEUE"])
        
        config_params["GAME_FILE_PATH"] = os.getenv("GAME_FILE_PATH", config["DEFAULT"]["GAME_FILE_PATH"])
        config_params["REVIEWS_FILE_PATH"] = os.getenv("REVIEWS_FILE_PATH", config["DEFAULT"]["REVIEWS_FILE_PATH"])
        
        config_params["LOGGING_LEVEL"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["SENDING_WAIT_TIME"] = int(os.getenv('SENDING_WAIT_TIME', config["DEFAULT"]["SENDING_WAIT_TIME"]))

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

    client = Client(config)

    client.run()


main()