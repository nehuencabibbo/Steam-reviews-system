import logging
import sys
import yaml
import json
import os
from typing import *


CLIENTS_PORT = 7777

# WATCHDOG
WATCHDOG_PORT = 8080
WATCHDOG_ELECTION_PORT = 9000
WATCHDOG_LEADER_COMUNICATION_PORT = 9500
WAIT_BETWEEN_HEARTBEAT = 5.0
AMOUNT_OF_WATCHDOGS = 3
LEADER_DISCOVERY_PORT = 10015

# ALL QUERIES
AMOUNT_OF_FILTER_COLUMNS = 1
AMOUNT_OF_DROP_NULLS = 1
# Q2
Q2_AMOUNT_OF_INDIE_GAMES_FILTERS = 2
Q2_AMOUNT_OF_GAMES_FROM_LAST_DECADE_FILTERS = 2
Q2_AMOUNT_OF_TOP_K_NODES = 2
# Q3
Q3_AMOUNT_OF_INDIE_GAMES_FILTERS = 2
Q3_AMOUNT_OF_POSITIVE_REVIEWS_FILTERS = 4
Q3_AMOUNT_OF_COUNTERS_BY_APP_ID = 5
Q3_AMOUNT_OF_TOP_K_NODES = 3
Q3_AMOUNT_OF_JOINS = 2
# Q4
Q4_AMOUNT_OF_ACTION_GAMES_FILTERS = 2
Q4_AMOUNT_OF_NEGATIVE_REVIEWS_FILTERS = 3
Q4_AMOUNT_OF_FIRST_COUNTER_BY_APP_ID = 2
Q4_AMOUNT_OF_SECOND_COUNTER_BY_APP_ID = 3
Q4_AMOUNT_OF_ENGLISH_REVIEWS_FILTERS = 1
Q4_AMOUNT_OF_FIRST_MORE_THAN_5000_FILTERS = 2
Q4_AMOUNT_OF_SECOND_MORE_THAN_5000_FILTERS = 2
Q4_AMOUNT_OF_FIRST_JOINS = 2
Q4_AMOUNT_OF_SECOND_JOINS = 2
Q4_AMOUNT_OF_THIRD_JOINS = 1  # TODO: NEEDS AGGREGATOR FOR SCALING THIS NODE
# Q5
Q5_AMOUNT_OF_ACTION_GAMES_FILTERS = 2
Q5_AMOUNT_OF_NEGATIVE_REVIEWS_FILTERS = 3
Q5_AMOUNT_OF_COUNTERS = 2
Q5_AMOUNT_OF_JOINS = 2
Q5_AMOUNT_OF_PERCENTILES = 1


def create_file(output, file_name):
    with open(file_name, "w") as output_file:
        yaml.safe_dump(output, output_file, sort_keys=False, default_flow_style=False)


def create_names_file(node_names, monitor_names):

    os.makedirs("watchdog_status", exist_ok=True)

    with open("watchdog_status/node_names.json", "w") as file:
        json.dump(node_names, file)

    with open("watchdog_status/monitor_names.json", "w") as file:
        json.dump(monitor_names, file)


def add_networks(networks: Dict):
    networks["net"] = {}
    networks["net"]["ipam"] = {}
    networks["net"]["ipam"]["driver"] = "default"
    networks["net"]["ipam"]["config"] = [{"subnet": "172.25.125.0/24"}]


def add_volumes(output: Dict):
    output["volumes"] = {"rabbitmq_data": {}}


def add_watchdog(output, port, debug=False, num=1):

    output["services"][f"watchdog_{num}"] = {
        "image": "watchdog:latest",
        "container_name": f"watchdog_{num}",
        "volumes": [
            "/var/run/docker.sock:/var/run/docker.sock",
            "./watchdog_status/:/watchdog_status",
        ],
        "environment": [
            f"NODE_ID={num}",
            f"LOGGING_LEVEL={'INFO' if not debug else 'DEBUG'}",
            f"PORT={port}",
            f"WAIT_BETWEEN_HEARTBEAT={WAIT_BETWEEN_HEARTBEAT}",
            f"ELECTION_PORT= {WATCHDOG_ELECTION_PORT}",
            f"LEADER_COMUNICATION_PORT={WATCHDOG_LEADER_COMUNICATION_PORT}",
            f"AMOUNT_OF_MONITORS={AMOUNT_OF_WATCHDOGS}",
            f"LEADER_DISCOVERY_PORT={LEADER_DISCOVERY_PORT}",
        ],
        "networks": ["net"],
    }


def add_filter_columns(output: Dict, num: int, debug: bool):
    output["services"][f"filter_columns{num}"] = {
        "image": "filter_columns:latest",
        "container_name": f"filter_columns{num}",
        "environment": [
            f"NODE_ID={num}",
            f"INSTANCES_OF_MYSELF={AMOUNT_OF_FILTER_COLUMNS}",
            f"LOGGING_LEVEL={'INFO' if not debug else 'DEBUG'}",
            f"WATCHDOG_PORT={WATCHDOG_PORT}",
            f"WATCHDOGS_IP={','.join([f'watchdog_{i}' for i in range(AMOUNT_OF_WATCHDOGS)])}",
            f"NODE_NAME={f'filter_columns{num}'}",
            f"LEADER_DISCOVERY_PORT={LEADER_DISCOVERY_PORT}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def add_drop_nulls(output: Dict, num: int, debug: bool):
    output["services"][f"drop_nulls{num}"] = {
        "image": "drop_nulls:latest",
        "container_name": f"drop_nulls{num}",
        "environment": [
            f"NODE_ID={num}",
            "COUNT_BY_PLATFORM_NODES=1",  # TODO: change when scaling
            f"INSTANCES_OF_MYSELF={AMOUNT_OF_DROP_NULLS}",
            f"LOGGING_LEVEL={'INFO' if not debug else 'DEBUG'}",
            f"WATCHDOG_PORT={WATCHDOG_PORT}",
            f"WATCHDOGS_IP={','.join([f'watchdog_{i}' for i in range(AMOUNT_OF_WATCHDOGS)])}",
            f"NODE_NAME={f'drop_nulls{num}'}",
            f"LEADER_DISCOVERY_PORT={LEADER_DISCOVERY_PORT}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def add_counter_by_platform(
    output: Dict,
    query: str,
    num: int,
    consume_queue_sufix: str,
    publish_queue: str,
    debug: bool,
    node_names: list,
):
    node_names.append(f"{query}_counter{num}")  # move to generator if any
    output["services"][f"{query}_counter{num}"] = {
        "container_name": f"{query}_counter{num}",
        "image": "counter_by_platform:latest",
        "entrypoint": "python3 /main.py",
        "environment": [
            f"NODE_ID={num}",
            f"CONSUME_QUEUE_SUFIX={consume_queue_sufix}",
            f"PUBLISH_QUEUE={publish_queue}",
            f"LOGGING_LEVEL={'DEBUG' if debug else 'INFO'}",
            f"WATCHDOG_PORT={WATCHDOG_PORT}",
            f"WATCHDOGS_IP={','.join([f'watchdog_{i}' for i in range(AMOUNT_OF_WATCHDOGS)])}",
            f"NODE_NAME={f'{query}_counter{num}'}",
            f"LEADER_DISCOVERY_PORT={LEADER_DISCOVERY_PORT}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def add_counter_by_app_id(
    output: Dict,
    query: str,
    num: int,
    consume_queue_sufix: str,
    publish_queue: str,
    amount_of_forwarding_queues: int,
    needed_ends: int,
    debug: bool,
    node_names: list,
):
    node_names.append(f"{query}_counter{num}")
    output["services"][f"{query}_counter{num}"] = {
        "container_name": f"{query}_counter{num}",
        "image": "counter_by_app_id:latest",
        "entrypoint": "python3 /main.py",
        "environment": [
            f"NODE_ID={num}",
            f"CONSUME_QUEUE_SUFIX={consume_queue_sufix}",
            f"PUBLISH_QUEUE={publish_queue}",
            f"LOGGING_LEVEL={'INFO' if not debug else 'DEBUG'}",
            f"AMOUNT_OF_FORWARDING_QUEUES={amount_of_forwarding_queues}",
            f"NEEDED_ENDS={needed_ends}",
            f"WATCHDOG_PORT={WATCHDOG_PORT}",
            f"WATCHDOGS_IP={','.join([f'watchdog_{i}' for i in range(AMOUNT_OF_WATCHDOGS)])}",
            f"NODE_NAME={f'{query}_counter{num}'}",
            f"LEADER_DISCOVERY_PORT={LEADER_DISCOVERY_PORT}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def add_top_k(
    output: Dict,
    query: str,
    num: int,
    input_top_k_queue_name: str,
    output_top_k_queue_name: str,
    k: int,
    amount_of_receiving_queues: int,
    debug: bool,
    node_names: list,
):
    node_names.append(f"{query}_top_k{num}")
    output["services"][f"{query}_top_k{num}"] = {
        "image": "top_k:latest",
        "container_name": f"{query}_top_k{num}",
        "environment": [
            f"INPUT_TOP_K_QUEUE_NAME={input_top_k_queue_name}",
            f"OUTPUT_TOP_K_QUEUE_NAME={output_top_k_queue_name}",
            f"K={k}",
            f"NODE_ID={num}",
            f"AMOUNT_OF_RECEIVING_QUEUES={amount_of_receiving_queues}",
            f"LOGGING_LEVEL={'INFO' if not debug else 'DEBUG'}",
            f"WATCHDOG_PORT={WATCHDOG_PORT}",
            f"WATCHDOGS_IP={','.join([f'watchdog_{i}' for i in range(AMOUNT_OF_WATCHDOGS)])}",
            f"NODE_NAME={f'{query}_top_k{num}'}",
            f"LEADER_DISCOVERY_PORT={LEADER_DISCOVERY_PORT}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def add_top_k_aggregator(
    output: Dict,
    query: str,
    num: int,
    input_top_k_aggregator_queue_name: str,
    output_top_k_aggregator_queue_name: str,
    k: int,
    amount_of_top_k_nodes: int,
    debug: bool,
    node_names: list,
):
    node_names.append(f"{query}_top_k{num}")
    output["services"][f"{query}_top_k{num}"] = {
        "image": "top_k:latest",
        "container_name": f"{query}_top_k{num}",
        "environment": [
            f"INPUT_TOP_K_QUEUE_NAME={input_top_k_aggregator_queue_name}",
            f"OUTPUT_TOP_K_QUEUE_NAME={output_top_k_aggregator_queue_name}",
            f"K={k}",
            f"NODE_ID={num}",
            f"AMOUNT_OF_RECEIVING_QUEUES={amount_of_top_k_nodes}",
            f"LOGGING_LEVEL={'INFO' if not debug else 'DEBUG'}",
            f"WATCHDOG_PORT={WATCHDOG_PORT}",
            f"WATCHDOGS_IP={','.join([f'watchdog_{i}' for i in range(AMOUNT_OF_WATCHDOGS)])}",
            f"NODE_NAME={f'{query}_top_k{num}'}",
            f"LEADER_DISCOVERY_PORT={LEADER_DISCOVERY_PORT}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def add_filter_by_language(
    output: Dict,
    query: str,
    num: int,
    filter_name: str,
    input_queue_name: str,
    output_queue_name: str,
    amount_of_forwarding_queues: int,
    column_number_to_use: int,
    language: str,
    columns_to_keep: str,
    instances_of_myself: str,
    debug: bool,
    node_names: list,
    batch_size: int = 10,
    prefetch_count=100,
):
    node_names.append(f"{query}_filter_{filter_name}{num}")
    output["services"][f"{query}_filter_{filter_name}{num}"] = {
        "image": "filter_by_language:latest",
        "container_name": f"{query}_filter_{filter_name}{num}",
        "environment": [
            f"NODE_ID={num}",
            f"RECIVING_QUEUE_NAME={input_queue_name}",
            f"FORWARDING_QUEUE_NAMES={output_queue_name}",
            f"AMOUNT_OF_FORWARDING_QUEUES={amount_of_forwarding_queues}",
            f"LOGGING_LEVEL={'INFO' if not debug else 'DEBUG'}",
            f"COLUMN_NUMBER_TO_USE={column_number_to_use}",
            f"LANGUAGE={language}",
            f"COLUMNS_TO_KEEP={columns_to_keep}",
            f"INSTANCES_OF_MYSELF={instances_of_myself}",
            f"BATCH_SIZE={batch_size}",
            f"PREFETCH_COUNT={prefetch_count}",
            f"WATCHDOG_PORT={WATCHDOG_PORT}",
            f"WATCHDOGS_IP={','.join([f'watchdog_{i}' for i in range(AMOUNT_OF_WATCHDOGS)])}",
            f"NODE_NAME={f'{query}_filter_{filter_name}{num}'}",
            f"LEADER_DISCOVERY_PORT={LEADER_DISCOVERY_PORT}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def add_filter_by_value(
    output: Dict,
    query: str,
    num: int,
    filter_name: str,
    input_queue_name: str,
    output_queue_name: str,
    amount_of_forwarding_queues: int,
    column_number_to_use: int,
    value_to_filter_by: str,
    criteria: str,
    columns_to_keep: str,
    instances_of_myself: str,
    debug: bool,
    node_names: list,
    batch_size: int = 10,
    prefetch_count=100,
):
    node_names.append(f"{query}_filter_{filter_name}{num}")
    output["services"][f"{query}_filter_{filter_name}{num}"] = {
        "image": "filter_by_column_value:latest",
        "container_name": f"{query}_filter_{filter_name}{num}",
        "environment": [
            f"NODE_ID={num}",
            f"RECIVING_QUEUE_NAME={input_queue_name}",
            f"FORWARDING_QUEUE_NAMES={output_queue_name}",
            f"AMOUNT_OF_FORWARDING_QUEUES={amount_of_forwarding_queues}",
            f"LOGGING_LEVEL={'INFO' if not debug else 'DEBUG'}",
            f"COLUMN_NUMBER_TO_USE={column_number_to_use}",
            f"VALUE_TO_FILTER_BY={value_to_filter_by}",
            f"CRITERIA={criteria}",
            f"COLUMNS_TO_KEEP={columns_to_keep}",
            f"INSTANCES_OF_MYSELF={instances_of_myself}",
            f"BATCH_SIZE={batch_size}",
            f"PREFETCH_COUNT={prefetch_count}",
            f"WATCHDOG_PORT={WATCHDOG_PORT}",
            f"WATCHDOGS_IP={','.join([f'watchdog_{i}' for i in range(AMOUNT_OF_WATCHDOGS)])}",
            f"NODE_NAME={f'{query}_filter_{filter_name}{num}'}",
            f"LEADER_DISCOVERY_PORT={LEADER_DISCOVERY_PORT}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def add_join(
    output: Dict,
    query: str,
    num: int,
    input_games_queue_name: str,
    input_reviews_queue_name: str,
    output_queue_name: str,
    needed_reviews_ends: int,
    needed_games_ends: int,
    amount_of_forwarding_queues: int,
    games_columns_to_keep: str,
    reviews_columns_to_keep: str,
    debug: bool,
    instances_of_myself: int,
    node_names: list,
):
    node_names.append(f"{query}_join{num}")
    output["services"][f"{query}_join{num}"] = {
        "container_name": f"{query}_join{num}",
        "image": "join:latest",
        "environment": [
            f"NODE_ID={num}",
            f"INPUT_GAMES_QUEUE_NAME={num}_{input_games_queue_name}",
            f"INPUT_REVIEWS_QUEUE_NAME={num}_{input_reviews_queue_name}",
            f"OUTPUT_QUEUE_NAME={output_queue_name}",
            f"NEEDED_GAMES_ENDS={needed_games_ends}",
            f"NEEDED_REVIEWS_ENDS={needed_reviews_ends}",
            f"AMOUNT_OF_FORWARDING_QUEUES={amount_of_forwarding_queues}",
            f"LOGGING_LEVEL={'INFO' if not debug else 'DEBUG'}",
            f"GAMES_COLUMNS_TO_KEEP={games_columns_to_keep}",
            f"REVIEWS_COLUMNS_TO_KEEP={reviews_columns_to_keep}",
            f"INSTANCES_OF_MYSELF={instances_of_myself}",
            f"WATCHDOG_PORT={WATCHDOG_PORT}",
            f"WATCHDOGS_IP={','.join([f'watchdog_{i}' for i in range(AMOUNT_OF_WATCHDOGS)])}",
            f"NODE_NAME={f'{query}_join{num}'}",
            f"LEADER_DISCOVERY_PORT={LEADER_DISCOVERY_PORT}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def add_percentile(
    output: Dict,
    query: str,
    num: int,
    consume_queue: str,
    publish_queue: str,
    needed_ends_to_finish: int,
    debug: bool,
    node_names: list,
):
    node_names.append(f"{query}_percentile_{num}")
    output["services"][f"{query}_percentile_{num}"] = {
        "container_name": f"{query}_percentile_{num}",
        "image": "percentile:latest",
        "entrypoint": "python3 /main.py",
        "environment": [
            f"NODE_ID={num}",
            f"CONSUME_QUEUE={consume_queue}",
            f"PUBLISH_QUEUE={publish_queue}",
            f"LOGGING_LEVEL={'INFO' if not debug else 'DEBUG'}",
            f"NEEDED_ENDS_TO_FINISH={needed_ends_to_finish}",
            f"WATCHDOG_PORT={WATCHDOG_PORT}",
            f"WATCHDOGS_IP={','.join([f'watchdog_{i}' for i in range(AMOUNT_OF_WATCHDOGS)])}",
            f"NODE_NAME={f'{query}_percentile_{num}'}",
            f"LEADER_DISCOVERY_PORT={LEADER_DISCOVERY_PORT}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def generate_watchdogs(output, amount, monitor_names: list, debug=False):
    for i in range(amount):
        add_watchdog(output, port=WATCHDOG_PORT, debug=debug, num=i)
        monitor_names.append(f"watchdog_{i}")


def generate_drop_nulls(output: Dict, amount: int, node_names: list, debug=False):
    for i in range(amount):
        add_drop_nulls(output=output, num=i, debug=debug)
        node_names.append(f"drop_nulls{i}")


def generate_drop_columns(output: Dict, amount: int, node_names: list, debug=False):
    for i in range(amount):
        add_filter_columns(output=output, num=i, debug=debug)
        node_names.append(f"filter_columns{i}")


def add_rabbit(output: Dict):
    output["services"]["rabbitmq"] = {
        "image": "rabbitmq:3-management",
        "container_name": "rabbitmq",
        "ports": ["15672:15672"],
        "networks": ["net"],
        "volumes": ["rabbitmq_data:/var/lib/rabbitmq"],
        # "logging": {"driver": "none"},
        "healthcheck": {
            "test": "rabbitmq-diagnostics -q ping",
            "interval": "20s",
            "timeout": "10s",
            "retries": "5",
        },
    }


def add_client(
    output: Dict, num: int, games_file_path: str, reviews_file_path: str, debug: bool
):
    output["services"][f"client{num}"] = {
        "container_name": f"client{num}",
        "image": "client:latest",
        "entrypoint": "python3 /main.py",
        "environment": [
            f"LOGGING_LEVEL={'DEBUG' if debug else 'INFO'}",
            f"GAME_FILE_PATH={games_file_path}",
            f"REVIEWS_FILE_PATH={reviews_file_path}",
        ],
        "volumes": ["./data/:/data"],
        "networks": ["net"],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "restart": "on-failure",
    }


def add_client_handler(
    output: Dict, num: int, debug: bool, port: int, node_names: list
):
    node_names.append(f"client_handler{num}")  # move to generate... if any
    depends_on: dict[str, str] = {
        "rabbitmq": {"condition": "service_healthy"},
    }
    # for i in range(AMOUNT_OF_FILTER_COLUMNS):
    #     depends_on[f"filter_columns{i}"] = {"condition": "service_started"}

    logging.debug(f"depends_on: {depends_on}")

    output["services"][f"client_handler{num}"] = {
        "container_name": f"client_handler{num}",
        "image": "client_handler:latest",
        "entrypoint": "python3 /main.py",
        "environment": [
            f"LOGGING_LEVEL={'DEBUG' if debug else 'INFO'}",
            f"CLIENTS_PORT={port}",
            f"GAMES_QUEUE_NAME=games",  # see filter_columns/config.ini
            f"REVIEWS_QUEUE_NAME=reviews",  # see filter_columns/config.ini
            f"WATCHDOG_PORT={WATCHDOG_PORT}",
            f"WATCHDOGS_IP={','.join([f'watchdog_{i}' for i in range(AMOUNT_OF_WATCHDOGS)])}",
            f"NODE_NAME={f'client_handler{num}'}",  # name of the service not container
            f"LEADER_DISCOVERY_PORT={LEADER_DISCOVERY_PORT}",
        ],
        "volumes": ["./data/:/data"],
        "networks": ["net"],
        "depends_on": depends_on,
        "restart": "on-failure",
    }


def generate_filters_by_value(
    amount_of_filters: int,
    node_names: list,
    debug=False,
    **kwargs,
):
    for i in range(amount_of_filters):
        add_filter_by_value(**kwargs, num=i, debug=debug, node_names=node_names)


def generate_filters_by_language(
    amount_of_filters: int,
    node_names: list,
    debug=False,
    **kwargs,
):
    for i in range(amount_of_filters):
        add_filter_by_language(**kwargs, num=i, debug=debug, node_names=node_names)


def generate_counters_by_app_id(
    amount_of_counters: int, node_names: list, debug=False, **kwargs
):
    for i in range(amount_of_counters):
        add_counter_by_app_id(**kwargs, node_names=node_names, num=i, debug=debug)


def generate_tops_k(amount_of_tops_k: int, node_names: list, debug=False, **kwargs):
    for i in range(amount_of_tops_k):
        add_top_k(**kwargs, num=i, node_names=node_names, debug=debug)


def generate_joins(
    amount_of_joins: int,
    node_names: list,
    debug=False,
    **kwargs,
):
    for i in range(amount_of_joins):
        add_join(
            **kwargs,
            num=i,
            node_names=node_names,
            debug=debug,
            instances_of_myself=amount_of_joins,
        )


def generate_q1(node_names: list, output=Dict, debug=True):
    add_counter_by_platform(
        output=output,
        query="q1",
        num=0,
        consume_queue_sufix="q1_platform",
        publish_queue="Q1",
        debug=debug,
        node_names=node_names,
    )


def generate_q2(node_names: list, output=Dict, debug=False):
    # Recived message:
    #       client_id,
    #       message[GAMES_MSG_ID],
    #       message[GAMES_APP_ID],
    #       message[GAMES_NAME],
    #       message[GAMES_RELEASE_DATE],
    #       message[GAMES_AVG_PLAYTIME_FOREVER],
    #       message[GAMES_GENRE],

    q2_indie_filter_args = {
        "output": output,
        "query": "q2",
        "filter_name": "indie_games",
        "input_queue_name": "q2_games",
        "output_queue_name": "q2_indie_games",
        "amount_of_forwarding_queues": 1,
        "column_number_to_use": 6,  # genre
        "value_to_filter_by": "indie",
        "criteria": "CONTAINS",
        "columns_to_keep": "0,1,2,3,4,5",  # Todo menos el genero
        "instances_of_myself": Q2_AMOUNT_OF_INDIE_GAMES_FILTERS,
    }
    generate_filters_by_value(
        Q2_AMOUNT_OF_INDIE_GAMES_FILTERS,
        node_names,
        debug=debug,
        **q2_indie_filter_args,
    )

    # Recived message:
    #       client_id,
    #       message[GAMES_MSG_ID],
    #       message[GAMES_APP_ID],
    #       message[GAMES_NAME],
    #       message[GAMES_RELEASE_DATE],
    #       message[GAMES_AVG_PLAYTIME_FOREVER],

    q2_indie_games_from_last_decade_args = {
        "output": output,
        "query": "q2",
        "filter_name": "indie_games_from_last_decade",
        "input_queue_name": "0_q2_indie_games",
        "output_queue_name": "q2_indie_games_from_last_decade",
        "amount_of_forwarding_queues": Q2_AMOUNT_OF_TOP_K_NODES,
        "column_number_to_use": 4,  # release date
        "value_to_filter_by": 201,
        "criteria": "CONTAINS",
        "columns_to_keep": "0,1,3,5",  # client_id, msg_id, name, avg_forever
        "instances_of_myself": Q2_AMOUNT_OF_GAMES_FROM_LAST_DECADE_FILTERS,
    }

    generate_filters_by_value(
        Q2_AMOUNT_OF_GAMES_FROM_LAST_DECADE_FILTERS,
        node_names,
        debug=debug,
        **q2_indie_games_from_last_decade_args,
    )

    # Recived message:
    #       client_id,
    #       message[GAMES_MSG_ID],
    #       message[GAMES_NAME],
    #       message[GAMES_AVG_PLAYTIME_FOREVER],

    q2_top_k_args = {
        "output": output,
        "query": "q2",
        "input_top_k_queue_name": "q2_indie_games_from_last_decade",
        "output_top_k_queue_name": "0_q2_top_aggregator",
        "k": 10,
        "amount_of_receiving_queues": 1,  # As it comes from a filter (which sends an end when every filter has ended)
    }

    generate_tops_k(Q2_AMOUNT_OF_TOP_K_NODES, node_names, debug=debug, **q2_top_k_args)

    add_top_k_aggregator(
        output=output,
        query="q2_aggregator",
        num=0,
        input_top_k_aggregator_queue_name="q2_top_aggregator",
        output_top_k_aggregator_queue_name="Q2",
        k=10,
        amount_of_top_k_nodes=Q2_AMOUNT_OF_TOP_K_NODES,
        debug=debug,
        node_names=node_names,
    )


def generate_q3(node_names: list, output: Dict, debug=False):
    q3_filter_indie_games_args = {
        "output": output,
        "query": "q3",
        "filter_name": "indie_games",
        "input_queue_name": "q3_games",
        "output_queue_name": "q3_indie_games",
        "amount_of_forwarding_queues": Q3_AMOUNT_OF_JOINS,
        "column_number_to_use": 4,  # genre
        "value_to_filter_by": "indie",
        "criteria": "CONTAINS",
        "columns_to_keep": "0,1,2,3",  # client_id, msg_id, app_id, name
        "instances_of_myself": Q3_AMOUNT_OF_INDIE_GAMES_FILTERS,
    }

    generate_filters_by_value(
        Q3_AMOUNT_OF_INDIE_GAMES_FILTERS,
        node_names,
        debug=debug,
        **q3_filter_indie_games_args,
    )

    q3_filter_positive_args = {
        "output": output,
        "query": "q3",
        "filter_name": "filter_positive",
        "input_queue_name": "q3_reviews",
        "output_queue_name": "q3_positive_reviews",
        "amount_of_forwarding_queues": Q3_AMOUNT_OF_COUNTERS_BY_APP_ID,
        "column_number_to_use": 3,  # review_score
        "value_to_filter_by": 1.0,  # positive_review
        "criteria": "EQUAL_FLOAT",
        "columns_to_keep": "0,1,2",  # client_id, app_id, msg_id
        "instances_of_myself": Q3_AMOUNT_OF_POSITIVE_REVIEWS_FILTERS,
    }
    generate_filters_by_value(
        Q3_AMOUNT_OF_POSITIVE_REVIEWS_FILTERS,
        node_names,
        debug=debug,
        **q3_filter_positive_args,
    )

    q3_counter_by_app_id_args = {
        "output": output,
        "query": "q3",
        "consume_queue_sufix": "q3_positive_reviews",
        "publish_queue": "q3_positive_review_count",
        "amount_of_forwarding_queues": Q3_AMOUNT_OF_JOINS,
        "needed_ends": 1,  # filter sends only one end
    }
    generate_counters_by_app_id(
        Q3_AMOUNT_OF_COUNTERS_BY_APP_ID,
        node_names,
        debug=debug,
        **q3_counter_by_app_id_args,
    )

    q3_join_args = {
        "output": output,
        "query": "q3",
        "input_games_queue_name": "q3_indie_games",
        "input_reviews_queue_name": "q3_positive_review_count",
        "output_queue_name": "q3_join_by_app_id_result",
        "games_columns_to_keep": "2",  # name
        "reviews_columns_to_keep": "2",  # positive_review_count
        "needed_games_ends": 1,
        "needed_reviews_ends": Q3_AMOUNT_OF_COUNTERS_BY_APP_ID,
        "amount_of_forwarding_queues": Q3_AMOUNT_OF_TOP_K_NODES,
    }
    # See handle_reviews in join to see that client_id is not needed in reviews/games_columns_to_keep

    generate_joins(
        amount_of_joins=Q3_AMOUNT_OF_JOINS,
        node_names=node_names,
        debug=debug,
        **q3_join_args,
    )

    q3_tops_k_args = {
        "output": output,
        "query": "q3",
        "input_top_k_queue_name": "q3_join_by_app_id_result",
        "output_top_k_queue_name": "0_q3_top_aggregator",
        "k": 5,
        "amount_of_receiving_queues": Q3_AMOUNT_OF_JOINS,
    }

    generate_tops_k(
        Q3_AMOUNT_OF_TOP_K_NODES, node_names=node_names, debug=debug, **q3_tops_k_args
    )

    add_top_k_aggregator(
        output=output,
        query="q3_aggregator",
        num=0,
        input_top_k_aggregator_queue_name="q3_top_aggregator",
        output_top_k_aggregator_queue_name="Q3",
        k=5,
        amount_of_top_k_nodes=Q3_AMOUNT_OF_TOP_K_NODES,
        debug=debug,
        node_names=node_names,
    )


def generate_q4(node_names: list, output: Dict, debug=False):
    # ATENCION: los joins estan altamente acoplados a el orden en el que se generan,
    # por lo que, no se recomienda mover el orden de creacion
    q4_filter_action_games_args = {
        "output": output,
        "query": "q4",
        "filter_name": "action_games",
        "input_queue_name": "q4_games",
        "output_queue_name": ",".join(
            ["q4_action_games_for_join0", "q4_action_games_for_join2"]
        ),
        "amount_of_forwarding_queues": ",".join(
            [
                f"{Q4_AMOUNT_OF_FIRST_JOINS}",
                f"{Q4_AMOUNT_OF_THIRD_JOINS}",
            ]
        ),
        "column_number_to_use": 4,  # genre
        "value_to_filter_by": "action",
        "criteria": "CONTAINS",
        "columns_to_keep": "0,1,2,3",  # client_id, msg_id, app_id, name
        "instances_of_myself": Q4_AMOUNT_OF_ACTION_GAMES_FILTERS,
    }

    generate_filters_by_value(
        Q4_AMOUNT_OF_ACTION_GAMES_FILTERS,
        node_names=node_names,
        debug=debug,
        **q4_filter_action_games_args,
    )

    q4_filter_negative_reviews_args = {
        "output": output,
        "query": "q4",
        "filter_name": "negative",
        "input_queue_name": "q4_reviews",
        "output_queue_name": ",".join(
            ["q4_negative_reviews_for_counter0", "q4_negative_reviews_for_join1"]
        ),
        "amount_of_forwarding_queues": ",".join(
            [
                f"{Q4_AMOUNT_OF_FIRST_COUNTER_BY_APP_ID}",
                f"{Q4_AMOUNT_OF_SECOND_JOINS}",
            ]
        ),
        "column_number_to_use": 4,  # review_score
        "value_to_filter_by": -1.0,
        "criteria": "EQUAL_FLOAT",
        "columns_to_keep": "0,1,2,3",  # client_id, msg_id, app_id, review
        "instances_of_myself": Q4_AMOUNT_OF_NEGATIVE_REVIEWS_FILTERS,
        # "batch_size": ,
    }

    generate_filters_by_value(
        Q4_AMOUNT_OF_NEGATIVE_REVIEWS_FILTERS,
        node_names=node_names,
        debug=debug,
        **q4_filter_negative_reviews_args,
    )

    q4_negative_reviews_counter_args = {
        "output": output,
        "query": "q4_first_counter",
        "consume_queue_sufix": "q4_negative_reviews_for_counter0",
        "publish_queue": "q4_negative_reviews_count",
        "amount_of_forwarding_queues": 1,  # As it's a filter (working queue)
        "needed_ends": 1,  # onluy receives one end from the filter
    }
    generate_counters_by_app_id(
        Q4_AMOUNT_OF_FIRST_COUNTER_BY_APP_ID,
        node_names=node_names,
        debug=debug,
        **q4_negative_reviews_counter_args,
    )

    q4_filter_more_than_5000_args = {
        "output": output,
        "query": "q4",
        "filter_name": "first_more_than_5000_",
        "input_queue_name": "0_q4_negative_reviews_count",
        "output_queue_name": "q4_filter_first_more_than_5000_reviews",
        "amount_of_forwarding_queues": f"{Q4_AMOUNT_OF_FIRST_JOINS}",
        "column_number_to_use": 3,  # positive_review_count
        "value_to_filter_by": 5000,
        "criteria": "GREATER_THAN",
        "columns_to_keep": "0,1,2",  # client_id, msg_id, app_id
        "instances_of_myself": Q4_AMOUNT_OF_FIRST_MORE_THAN_5000_FILTERS,
    }
    generate_filters_by_value(
        Q4_AMOUNT_OF_FIRST_MORE_THAN_5000_FILTERS,
        node_names=node_names,
        debug=debug,
        **q4_filter_more_than_5000_args,
    )

    q4_first_join_args = {
        "output": output,
        "query": "q4_first",
        "input_games_queue_name": "q4_action_games_for_join0",
        "input_reviews_queue_name": "q4_filter_first_more_than_5000_reviews",
        "output_queue_name": "q4_first_join",
        "needed_games_ends": 1,  # It recives from a filter that doesn't have a counter behind
        "needed_reviews_ends": Q4_AMOUNT_OF_FIRST_COUNTER_BY_APP_ID,  # Q4_AMOUNT_OF_FIRST_MORE_THAN_5000_FILTERS,
        "amount_of_forwarding_queues": Q4_AMOUNT_OF_SECOND_JOINS,
        "games_columns_to_keep": "1,2",  # app_id, name
        "reviews_columns_to_keep": "",
    }

    generate_joins(
        amount_of_joins=Q4_AMOUNT_OF_FIRST_JOINS,
        node_names=node_names,
        debug=debug,
        **q4_first_join_args,
    )

    # add_join(
    #     output=output,
    #     query="q4",
    #     num=0,
    #     input_games_queue_name="0_q4_action_games",
    #     input_reviews_queue_name="0_q4_filter_first_more_than_5000_reviews",
    #     output_queue_name="q4_first_join",
    #     amount_of_behind_nodes=1,  # 1 as the filters work as a group (will receive only one end from them)
    #     amount_of_forwarding_queues=1,  # 1 as the filters work as a group (will receive only one end from them)
    #     games_columns_to_keep="0,1",  # app_id, name
    #     reviews_columns_to_keep="",  #
    # )

    q4_second_join_args = {
        "output": output,
        "query": "q4_second",
        "input_games_queue_name": "q4_first_join",
        "input_reviews_queue_name": "q4_negative_reviews_for_join1",
        "output_queue_name": "q4_second_join",
        "needed_games_ends": Q4_AMOUNT_OF_FIRST_JOINS,
        "needed_reviews_ends": 1,  # Filter that is not precceded by a counter -> They agree to send only one END
        "amount_of_forwarding_queues": 1,  # Filters recive from a single queue
        "games_columns_to_keep": "1",  # app_id
        "reviews_columns_to_keep": "2",  # review
    }

    generate_joins(
        amount_of_joins=Q4_AMOUNT_OF_SECOND_JOINS,
        node_names=node_names,
        debug=debug,
        **q4_second_join_args,
    )

    q4_filter_english_reviews_args = {
        "output": output,
        "query": "q4",
        "filter_name": "english",
        "input_queue_name": "0_q4_second_join",
        "output_queue_name": "q4_english_reviews",
        "amount_of_forwarding_queues": Q4_AMOUNT_OF_SECOND_COUNTER_BY_APP_ID,
        "column_number_to_use": 2,  # review
        "language": "en",
        "columns_to_keep": "0,1,2",  # client_id, msg_id, app_id
        "instances_of_myself": Q4_AMOUNT_OF_ENGLISH_REVIEWS_FILTERS,
        "prefetch_count": 1,
    }

    generate_filters_by_language(
        Q4_AMOUNT_OF_ENGLISH_REVIEWS_FILTERS,
        node_names=node_names,
        debug=debug,
        **q4_filter_english_reviews_args,
    )

    q4_english_reviews_counter_args = {
        "output": output,
        "query": "q4_second",  # q4_second_counter
        "consume_queue_sufix": "q4_english_reviews",
        "publish_queue": "q4_english_review_count",
        "amount_of_forwarding_queues": 1,  # Next node is a filter -> Filters consume from one queue exlusively
        "needed_ends": Q4_AMOUNT_OF_SECOND_JOINS,
    }
    generate_counters_by_app_id(
        Q4_AMOUNT_OF_SECOND_COUNTER_BY_APP_ID,
        node_names=node_names,
        debug=debug,
        **q4_english_reviews_counter_args,
    )

    q4_filter_more_than_5000_args = {
        "output": output,
        "query": "q4",
        "filter_name": "second_more_than_5000_",
        "input_queue_name": "0_q4_english_review_count",
        "output_queue_name": "q4_second_filter_more_than_5000",
        "amount_of_forwarding_queues": Q4_AMOUNT_OF_THIRD_JOINS,
        "column_number_to_use": 3,  # negative_english_review_count
        "value_to_filter_by": 5000,
        "criteria": "GREATER_THAN",
        "columns_to_keep": "0,1,2,3",  # client_id, msg_id, app_id, positive_review_count
        "instances_of_myself": Q4_AMOUNT_OF_SECOND_MORE_THAN_5000_FILTERS,
    }
    generate_filters_by_value(
        Q4_AMOUNT_OF_SECOND_MORE_THAN_5000_FILTERS,
        node_names=node_names,
        debug=debug,
        **q4_filter_more_than_5000_args,
    )

    q4_third_join_args = {
        "output": output,
        "query": "q4_third",
        "input_games_queue_name": "q4_action_games_for_join2",
        "input_reviews_queue_name": "q4_second_filter_more_than_5000",
        "output_queue_name": "Q4",
        # Counter followed by filter breaks filter algorithm,
        # now filters sends as many ENDs as instances of itself there are
        "needed_games_ends": 1,  # Q4_AMOUNT_OF_ACTION_GAMES_FILTERS,
        "needed_reviews_ends": Q4_AMOUNT_OF_SECOND_COUNTER_BY_APP_ID,
        "amount_of_forwarding_queues": 1,  # 1 as the filters work as a group (will receive only one end from them)
        "games_columns_to_keep": "2",  # app_id(0), name(1)
        "reviews_columns_to_keep": "",  # count(index: 1)
    }

    generate_joins(
        amount_of_joins=Q4_AMOUNT_OF_THIRD_JOINS,
        node_names=node_names,
        debug=debug,
        **q4_third_join_args,
    )

    # add_join(
    #     output=output,
    #     query="q4",
    #     num=2,
    #     input_games_queue_name="1_q4_action_games",
    #     input_reviews_queue_name="0_q4_second_filter_more_than_5000",
    #     output_queue_name="Q4",
    #     amount_of_behind_nodes=1,  # 1 as the filters work as a group (will receive only one end from them)
    #     amount_of_forwarding_queues=1,  # 1 as the filters work as a group (will receive only one end from them)
    #     games_columns_to_keep="0,1",  # app_id, name
    #     reviews_columns_to_keep="1",  # count
    # )


def generate_q5(node_names: list, output: Dict, debug=False):
    q5_filter_action_games_args = {
        "output": output,
        "query": "q5",
        "filter_name": "action_games",
        "input_queue_name": "q5_games",
        "output_queue_name": "q5_action_games",
        "amount_of_forwarding_queues": Q5_AMOUNT_OF_JOINS,
        "column_number_to_use": 4,  # client_id, msg_id, app_id, name, genre
        "value_to_filter_by": "action",
        "criteria": "CONTAINS",
        "columns_to_keep": "0,1,2,3",  # client_id, msg_id, app_id, name, genre
        "instances_of_myself": Q5_AMOUNT_OF_ACTION_GAMES_FILTERS,
    }
    generate_filters_by_value(
        Q5_AMOUNT_OF_ACTION_GAMES_FILTERS,
        node_names=node_names,
        debug=debug,
        **q5_filter_action_games_args,
    )

    q5_filter_negative_reviews_args = {
        "output": output,
        "query": "q5",
        "filter_name": "negative_reviews",
        "input_queue_name": "q5_reviews",
        "output_queue_name": "q5_negative_reviews",
        "amount_of_forwarding_queues": Q5_AMOUNT_OF_COUNTERS,
        "column_number_to_use": 3,  # client_id, msg_id, app_id, review_score
        "value_to_filter_by": -1.0,
        "criteria": "EQUAL_FLOAT",
        "columns_to_keep": "0,1,2",  # client_id, msg_id, app_id,
        "instances_of_myself": Q5_AMOUNT_OF_NEGATIVE_REVIEWS_FILTERS,
    }
    generate_filters_by_value(
        Q5_AMOUNT_OF_NEGATIVE_REVIEWS_FILTERS,
        node_names=node_names,
        debug=debug,
        **q5_filter_negative_reviews_args,
    )

    q5_negative_reviews_counter_args = {
        "output": output,
        "query": "q5",
        "consume_queue_sufix": "q5_negative_reviews",
        "publish_queue": "q5_counter",
        "amount_of_forwarding_queues": Q5_AMOUNT_OF_JOINS,
        "needed_ends": 1,  # filter sends only one end
    }

    generate_counters_by_app_id(
        Q5_AMOUNT_OF_COUNTERS,
        node_names=node_names,
        debug=debug,
        **q5_negative_reviews_counter_args,
    )

    q5_join = {
        "output": output,
        "query": "q5",
        "input_games_queue_name": "q5_action_games",
        "input_reviews_queue_name": "q5_counter",
        "output_queue_name": "q5_percentile",
        "needed_games_ends": 1,
        "needed_reviews_ends": Q5_AMOUNT_OF_COUNTERS,
        "amount_of_forwarding_queues": Q5_AMOUNT_OF_PERCENTILES,
        # TODO: esto se tiene que cambiar para que coincida con los demas nodos
        "games_columns_to_keep": "2",  # client_id, msg_id, app_id, name -> NO TENER EN CUENTA EL CLIENT ID PARA EL NUM
        "reviews_columns_to_keep": "2",  # client_id, msg_id, count -> IDEM ARRIBA
    }

    generate_joins(
        amount_of_joins=Q5_AMOUNT_OF_JOINS,
        node_names=node_names,
        debug=debug,
        **q5_join,
    )

    # TODO: Add generate percentile so there can be more than one instance of this
    add_percentile(
        output=output,
        query="q5",
        num=0,
        consume_queue="0_q5_percentile",  # TODO: Change if scalation of this node is implemented
        publish_queue="Q5",
        needed_ends_to_finish=Q5_AMOUNT_OF_JOINS,
        debug=debug,
        node_names=node_names,
    )


def generate_output(node_names: list, monitor_names: list):
    output = {}

    output["name"] = "steam_reviews_system"

    output["services"] = {}
    add_rabbit(output)

    generate_watchdogs(output, AMOUNT_OF_WATCHDOGS, monitor_names, debug=False)
    # GAME_FILE_PATH=data/games_sample.csv
    # REVIEWS_FILE_PATH=data/reviews_sample.csv

    # ; GAME_FILE_PATH=data/filtered_games.csv
    # ; REVIEWS_FILE_PATH=data/filtered_reviews.csv

    # add_client(
    #     output,
    #     num=1,
    #     # games_file_path="data/games.csv",
    #     # reviews_file_path="data/reviews_sample.csv",
    #     games_file_path="data/games_sample.csv",
    #     reviews_file_path="data/reviews_sample.csv",
    #     debug=False,
    # )
    add_client(
        output,
        num=2,
        # games_file_path="data/games_sample.csv",
        # reviews_file_path="data/reviews_sample.csv",
        games_file_path="data/games.csv",
        reviews_file_path="data/filtered_reviews.csv",
        debug=False,
    )
    add_client_handler(
        output=output, num=1, debug=False, port=CLIENTS_PORT, node_names=node_names
    )
    generate_drop_columns(
        output, AMOUNT_OF_FILTER_COLUMNS, debug=False, node_names=node_names
    )
    generate_drop_nulls(
        output, AMOUNT_OF_DROP_NULLS, debug=False, node_names=node_names
    )

    # -------------------------------------------- Q1 -----------------------------------------
    # generate_q1(output=output, debug=False, node_names=node_names)
    # # -------------------------------------------- Q2 -----------------------------------------
    # generate_q2(output=output, debug=False, node_names=node_names)
    # # -------------------------------------------- Q3 -----------------------------------------
    # generate_q3(output=output, debug=False, node_names=node_names)
    # # -------------------------------------------- Q4 -----------------------------------------
    generate_q4(output=output, debug=True, node_names=node_names)
    # # -------------------------------------------- Q5 -----------------------------------------
    # generate_q5(output=output, debug=False, node_names=node_names)
    # -------------------------------------------- END OF QUERIES -----------------------------------------

    add_volumes(output=output)

    output["networks"] = {}
    add_networks(output["networks"])

    return output


def main():
    output_file_name = sys.argv[1]
    # Recive the rest of the needed config params

    node_names = []
    monitor_names = []
    output = generate_output(node_names, monitor_names)

    create_file(output, output_file_name)
    create_names_file(node_names, monitor_names)

    print(f"{output_file_name} was successfully created")


main()


# bc9d972d-6988-4d81-be3f-d71def936737 este es el q funca
# e487840a-3511-4291-a315-d65a5145114e este es el q no
