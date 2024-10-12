import sys
import yaml
from typing import *


AMOUNT_OF_DROP_FILTER_COLUMNS = 5
AMOUNT_OF_DROP_NULLS = 5
# Q2
Q2_AMOUNT_OF_INDIE_GAMES_FILTERS = 2
Q2_AMOUNT_OF_GAMES_FROM_LAST_DECADE_FILTERS = 2
Q2_AMOUNT_OF_TOP_K_NODES = 2
# Q3
Q3_AMOUNT_OF_INDIE_GAMES_FILTERS = 2
Q3_AMOUNT_OF_POSITIVE_REVIEWS_FILTERS = 2
Q3_AMOUNT_OF_COUNTERS_BY_APP_ID = 5
Q3_AMOUNT_OF_TOP_K_NODES = 3
# Q4
Q4_AMOUNT_OF_ACTION_GAMES_FILTERS = 1
Q4_AMOUNT_OF_NEGATIVE_REVIEWS_FILTERS = 1
Q4_AMOUNT_OF_COUNTERS_BY_APP_ID = 1
Q4_AMOUNT_OF_ENGLISH_REVIEWS_FILTERS = 1
Q4_AMOUNT_OF_MORE_THAN_5000_FILTERS = 1
# Q5
Q5_AMOUNT_OF_ACTION_GAMES_FILTERS = 3
Q5_AMOUNT_OF_NEGATIVE_REVIEWS_FILTERS = 3
Q5_AMOUNT_OF_COUNTERS_BY_APP_ID = 5


def create_file(output, file_name):
    with open(file_name, "w") as output_file:
        yaml.safe_dump(output, output_file, sort_keys=False, default_flow_style=False)


def add_networks(networks: Dict):
    networks["net"] = {}
    networks["net"]["ipam"] = {}
    networks["net"]["ipam"]["driver"] = "default"
    networks["net"]["ipam"]["config"] = [{"subnet": "172.25.125.0/24"}]


def add_volumes(output: Dict):
    output["volumes"] = {"rabbitmq_data": {}}


def add_filter_columns(output: Dict, num: int):
    output["services"][f"filter_columns{num}"] = {
        "image": "filter_columns:latest",
        "container_name": f"filter_columns{num}",
        "environment": [
            f"NODE_ID={num}",
            f"INSTANCES_OF_MYSELF={AMOUNT_OF_DROP_FILTER_COLUMNS}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def add_drop_nulls(output: Dict, num: int):
    output["services"][f"drop_columns{num}"] = {
        "image": "drop_nulls:latest",
        "container_name": f"drop_nulls{num}",
        "environment": [
            f"NODE_ID={num}",
            "COUNT_BY_PLATFORM_NODES=1",  # TODO: change when scaling
            f"INSTANCES_OF_MYSELF={AMOUNT_OF_DROP_NULLS}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def add_counter_by_platform(
    output: Dict, query: str, num: int, consume_queue_sufix: str, publish_queue: str
):
    output["services"][f"{query}_counter{num}"] = {
        "container_name": f"{query}_counter{num}",
        "image": "counter_by_platform:latest",
        "entrypoint": "python3 /main.py",
        "environment": [
            f"NODE_ID={num}",
            f"CONSUME_QUEUE_SUFIX={consume_queue_sufix}",
            f"PUBLISH_QUEUE={publish_queue}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def add_counter_by_app_id(
    output: Dict, query: str, num: int, consume_queue_sufix: str, publish_queue: str
):
    output["services"][f"{query}_counter{num}"] = {
        "container_name": f"{query}_counter{num}",
        "image": "counter_by_app_id:latest",
        "entrypoint": "python3 /main.py",
        "environment": [
            f"NODE_ID={num}",
            f"CONSUME_QUEUE_SUFIX={consume_queue_sufix}",
            f"PUBLISH_QUEUE={publish_queue}",
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
):
    output["services"][f"{query}_top_k{num}"] = {
        "image": "top_k:latest",
        "container_name": f"{query}_top_k{num}",
        "environment": [
            f"INPUT_TOP_K_QUEUE_NAME={input_top_k_queue_name}",
            f"OUTPUT_TOP_K_QUEUE_NAME={output_top_k_queue_name}",
            f"K={k}",
            f"NODE_ID={num}",
            f"AMOUNT_OF_RECEIVING_QUEUES=1",
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
):
    output["services"][f"{query}_top_k{num}"] = {
        "image": "top_k:latest",
        "container_name": f"{query}_top_k{num}",
        "environment": [
            f"INPUT_TOP_K_QUEUE_NAME={input_top_k_aggregator_queue_name}",
            f"OUTPUT_TOP_K_QUEUE_NAME={output_top_k_aggregator_queue_name}",
            f"K={k}",
            f"NODE_ID={num}",
            f"AMOUNT_OF_RECEIVING_QUEUES={amount_of_top_k_nodes}",
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
    logging_level: str,
    column_number_to_use: int,
    value_to_filter_by: str,
    criteria: str,
    columns_to_keep: str,
    instances_of_myself: str,
    batch_size: int = 10,
    broadcasts=False,
):
    output["services"][f"{query}_filter_{filter_name}{num}"] = {
        "image": "filter_by_column_value:latest",
        "container_name": f"{query}_{filter_name}{num}",
        "environment": [
            f"NODE_ID={num}",
            f"RECIVING_QUEUE_NAME={input_queue_name}",
            f"FORWARDING_QUEUE_NAME={output_queue_name}",
            f"AMOUNT_OF_FORWARDING_QUEUES={amount_of_forwarding_queues}",
            f"LOGGING_LEVEL={logging_level}",
            f"COLUMN_NUMBER_TO_USE={column_number_to_use}",
            f"VALUE_TO_FILTER_BY={value_to_filter_by}",
            f"CRITERIA={criteria}",
            f"COLUMNS_TO_KEEP={columns_to_keep}",
            f"INSTANCES_OF_MYSELF={instances_of_myself}",
            f"BATCH_SIZE={batch_size}",
            f"BROADCASTS={broadcasts}",
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
    amount_of_behind_nodes: int,
    amount_of_forwarding_queues: int,
):
    output["services"][f"{query}_join{num}"] = {
        "container_name": f"{query}_join{num}",
        "image": "join:latest",
        "environment": [
            f"INPUT_GAMES_QUEUE_NAME={input_games_queue_name}",
            f"INPUT_REVIEWS_QUEUE_NAME={input_reviews_queue_name}",
            f"OUTPUT_QUEUE_NAME={output_queue_name}",
            f"AMOUNT_OF_BEHIND_NODES={amount_of_behind_nodes}",
            f"AMOUNT_OF_FORWARDING_QUEUES={amount_of_forwarding_queues}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def add_percentile(
    output: Dict, query: str, num: int, consume_queue: str, publish_queue: str
):
    output["services"][f"{query}_percentil_{num}"] = {
        "container_name": f"{query}_percentile_{num}",
        "image": "percentile:latest",
        "entrypoint": "python3 /main.py",
        "environment": [
            f"NODE_ID={num}",
            f"CONSUME_QUEUE={consume_queue}",
            f"PUBLISH_QUEUE={publish_queue}",
        ],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "networks": ["net"],
        "restart": "on-failure",
    }


def generate_drop_nulls(output: Dict, amount: int):
    for i in range(amount):
        add_drop_nulls(output=output, num=i)


def generate_drop_columns(output: Dict, amount: int):
    for i in range(amount):
        add_filter_columns(output=output, num=i)


def add_rabbit(output: Dict):
    output["services"]["rabbitmq"] = {
        "image": "rabbitmq:3-management",
        "container_name": "rabbitmq",
        "ports": ["15672:15672"],
        "networks": ["net"],
        "volumes": ["rabbitmq_data:/var/lib/rabbitmq"],
        "logging": {"driver": "none"},
        "healthcheck": {
            "test": "rabbitmq-diagnostics -q ping",
            "interval": "5s",
            "timeout": "10s",
            "retries": "5",
        },
    }


def add_client(output: Dict):
    output["services"]["client1"] = {
        "container_name": "client1",
        "image": "client:latest",
        "entrypoint": "python3 /main.py",
        "volumes": ["./data/:/data"],
        "networks": ["net"],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "restart": "on-failure",
    }


def generate_filters_by_value(
    amount_of_filters: int,
    **kwargs,
):
    for i in range(amount_of_filters):
        add_filter_by_value(**kwargs, num=i)


def generate_counters_by_app_id(amount_of_counters: int, **kwargs):
    for i in range(amount_of_counters):
        add_counter_by_app_id(**kwargs, num=i)


def generate_tops_k(amount_of_tops_k: int, **kwargs):
    for i in range(amount_of_tops_k):
        add_top_k(**kwargs, num=i)


def generate_q1(output=Dict):
    add_counter_by_platform(
        output=output,
        query="q1",
        num=0,
        consume_queue_sufix="q1_platform",
        publish_queue="Q1",
    )


def generate_q2(output=Dict):

    q2_indie_filter_args = {
        "output": output,
        "query": "q2",
        "filter_name": "indie_games",
        "input_queue_name": "q2_games",
        "output_queue_name": "q2_indie_games",
        "amount_of_forwarding_queues": 1,
        "logging_level": "INFO",
        "column_number_to_use": 4,  # genre
        "value_to_filter_by": "indie",
        "criteria": "CONTAINS",
        "columns_to_keep": "0,1,2,3",
        "instances_of_myself": Q2_AMOUNT_OF_INDIE_GAMES_FILTERS,
    }
    generate_filters_by_value(Q2_AMOUNT_OF_INDIE_GAMES_FILTERS, **q2_indie_filter_args)

    q2_indie_games_from_last_decade_args = {
        "output": output,
        "query": "q2",
        "filter_name": "indie_games_from_last_decade",
        "input_queue_name": "0_q2_indie_games",
        "output_queue_name": "q2_indie_games_from_last_decade",
        "amount_of_forwarding_queues": Q2_AMOUNT_OF_TOP_K_NODES,
        "logging_level": "INFO",
        "column_number_to_use": 2,  # release date
        "value_to_filter_by": 201,
        "criteria": "CONTAINS",
        "columns_to_keep": "1,3",  # name, avg_forever
        "instances_of_myself": Q2_AMOUNT_OF_GAMES_FROM_LAST_DECADE_FILTERS,
    }

    generate_filters_by_value(
        Q2_AMOUNT_OF_GAMES_FROM_LAST_DECADE_FILTERS,
        **q2_indie_games_from_last_decade_args,
    )

    q2_top_k_args = {
        "output": output,
        "query": "q2",
        "input_top_k_queue_name": "q2_indie_games_from_last_decade",
        "output_top_k_queue_name": "0_q2_top_aggregator",
        "k": 10,
    }

    generate_tops_k(Q2_AMOUNT_OF_TOP_K_NODES, **q2_top_k_args)

    add_top_k_aggregator(
        output=output,
        query="q2_aggregator",
        num=0,
        input_top_k_aggregator_queue_name="q2_top_aggregator",
        output_top_k_aggregator_queue_name="Q2",
        k=10,
        amount_of_top_k_nodes=Q2_AMOUNT_OF_TOP_K_NODES,
    )


def generate_q3(output: Dict):
    q3_filter_indie_games_args = {
        "output": output,
        "query": "q3",
        "filter_name": "indie_games",
        "input_queue_name": "q3_games",
        "output_queue_name": "q3_indie_games",
        "amount_of_forwarding_queues": 1,
        "logging_level": "INFO",
        "column_number_to_use": 2,  # genre
        "value_to_filter_by": "indie",
        "criteria": "CONTAINS",
        "columns_to_keep": "0,1",  # app_id, name
        "instances_of_myself": Q3_AMOUNT_OF_INDIE_GAMES_FILTERS,
    }

    generate_filters_by_value(
        Q3_AMOUNT_OF_INDIE_GAMES_FILTERS, **q3_filter_indie_games_args
    )

    q3_filter_positive_args = {
        "output": output,
        "query": "q3",
        "filter_name": "filter_positive",
        "input_queue_name": "q3_reviews",
        "output_queue_name": "q3_positive_reviews",
        "amount_of_forwarding_queues": Q3_AMOUNT_OF_COUNTERS_BY_APP_ID,
        "logging_level": "INFO",
        "column_number_to_use": 1,  # review_score
        "value_to_filter_by": 1.0,  # positive_review
        "criteria": "EQUAL_FLOAT",
        "columns_to_keep": 0,  # app_id ,
        "instances_of_myself": Q3_AMOUNT_OF_POSITIVE_REVIEWS_FILTERS,
    }
    generate_filters_by_value(
        Q3_AMOUNT_OF_POSITIVE_REVIEWS_FILTERS, **q3_filter_positive_args
    )

    q3_counter_by_app_id_args = {
        "output": output,
        "query": "q3",
        "consume_queue_sufix": "q3_positive_reviews",
        "publish_queue": "q3_positive_review_count",
    }
    generate_counters_by_app_id(
        Q3_AMOUNT_OF_COUNTERS_BY_APP_ID, **q3_counter_by_app_id_args
    )

    add_join(
        output=output,
        query="q3",
        num=0,
        input_games_queue_name="0_q3_indie_games",  # Prefixed as it comes from a filter
        input_reviews_queue_name="q3_positive_review_count",
        output_queue_name="q3_join_by_app_id_result",
        amount_of_behind_nodes=Q3_AMOUNT_OF_COUNTERS_BY_APP_ID,
        amount_of_forwarding_queues=Q3_AMOUNT_OF_TOP_K_NODES,
    )

    q3_tops_k_args = {
        "output": output,
        "query": "q3",
        "input_top_k_queue_name": "q3_join_by_app_id_result",
        "output_top_k_queue_name": "0_q3_top_aggregator",
        "k": 5,
    }

    generate_tops_k(Q3_AMOUNT_OF_TOP_K_NODES, **q3_tops_k_args)

    add_top_k_aggregator(
        output=output,
        query="q3_aggregator",
        num=0,
        input_top_k_aggregator_queue_name="q3_top_aggregator",
        output_top_k_aggregator_queue_name="Q3",
        k=5,
        amount_of_top_k_nodes=Q3_AMOUNT_OF_TOP_K_NODES,
    )

    # add_top_k(
    #     output=output,
    #     query="q3",
    #     num=0,
    #     input_top_k_queue_name="q3_join_by_app_id_result",
    #     output_top_k_queue_name="Q3",
    #     k=5,
    # )


def generate_q4(output: Dict):
    q4_filter_action_games_args = {
        "output": output,
        "query": "q4",
        "filter_name": "action_games",
        "input_queue_name": "q4_games",
        "output_queue_name": "q4_action_games",
        "amount_of_forwarding_queues": 2,
        "logging_level": "DEBUG",
        "column_number_to_use": 2,  # genre
        "value_to_filter_by": "action",
        "criteria": "CONTAINS",
        "columns_to_keep": "0,1",  # app_id, name
        "instances_of_myself": Q4_AMOUNT_OF_ACTION_GAMES_FILTERS,
        "broadcasts": True,
    }

    generate_filters_by_value(
        Q4_AMOUNT_OF_ACTION_GAMES_FILTERS, **q4_filter_action_games_args
    )

    q4_filter_negative_reviews_args = {
        "output": output,
        "query": "q4",
        "filter_name": "negative",
        "input_queue_name": "q4_reviews",
        "output_queue_name": "q4_negative_reviews",
        "amount_of_forwarding_queues": 2,
        "logging_level": "DEBUG",
        "column_number_to_use": 2,  # review_score
        "value_to_filter_by": -1.0,
        "criteria": "EQUAL_FLOAT",
        "columns_to_keep": "0,1",  # app_id, review
        "instances_of_myself": Q4_AMOUNT_OF_NEGATIVE_REVIEWS_FILTERS,
        # "batch_size": ,
        "broadcasts": True,
    }

    generate_filters_by_value(
        Q4_AMOUNT_OF_NEGATIVE_REVIEWS_FILTERS, **q4_filter_negative_reviews_args
    )

    q4_negative_reviews_counter_args = {
        "output": output,
        "query": "q4_first_counter",
        "consume_queue_sufix": "q4_negative_reviews",
        "publish_queue": "q4_negative_reviews_count",
    }
    generate_counters_by_app_id(
        Q4_AMOUNT_OF_COUNTERS_BY_APP_ID, **q4_negative_reviews_counter_args
    )

    q4_filter_more_than_5000_args = {
        "output": output,
        "query": "q4",
        "filter_name": "first_more_than_5000_",
        "input_queue_name": "q4_negative_reviews_count",
        "output_queue_name": "q4_filter_first_more_than_5000_reviews",
        "amount_of_forwarding_queues": 1,
        "logging_level": "DEBUG",
        "column_number_to_use": 1,  # positive_review_count
        "value_to_filter_by": 5000,
        "criteria": "GREATER_THAN",
        "columns_to_keep": "0",  # app_id
        "instances_of_myself": Q4_AMOUNT_OF_MORE_THAN_5000_FILTERS,
    }
    generate_filters_by_value(
        Q4_AMOUNT_OF_MORE_THAN_5000_FILTERS, **q4_filter_more_than_5000_args
    )

    add_join(
        output=output,
        query="q4",
        num=0,
        input_games_queue_name="0_q4_action_games",
        input_reviews_queue_name="0_q4_filter_first_more_than_5000_reviews",
        output_queue_name="q4_first_join",
        amount_of_behind_nodes=1,  # 1 as the filters work as a group (will receive only one end from them)
        amount_of_forwarding_queues=1,  # 1 as the filters work as a group (will receive only one end from them)
    )

    add_join(
        output=output,
        query="q4",
        num=1,
        input_games_queue_name="0_q4_first_join",
        input_reviews_queue_name="1_q4_negative_reviews",
        output_queue_name="q4_second_join",
        amount_of_behind_nodes=1,  # 1 as the filters work as a group (will receive only one end from them)
        amount_of_forwarding_queues=1,  # 1 as the filters work as a group (will receive only one end from them)
        # TODO: BATCHSIZE
    )

    q4_filter_english_reviews_args = {
        "output": output,
        "query": "q4",
        "filter_name": "english",
        "input_queue_name": "0_q4_second_join",
        "output_queue_name": "q4_english_reviews",
        "amount_of_forwarding_queues": 1,
        "logging_level": "DEBUG",
        "column_number_to_use": 2,  # review
        "value_to_filter_by": "en",
        "criteria": "LANGUAGE",
        "columns_to_keep": "0",  # app_id
        "instances_of_myself": Q4_AMOUNT_OF_ENGLISH_REVIEWS_FILTERS,
    }

    generate_filters_by_value(
        Q4_AMOUNT_OF_ENGLISH_REVIEWS_FILTERS, **q4_filter_english_reviews_args
    )

    q4_english_reviews_counter_args = {
        "output": output,
        "query": "q4_second_counter",
        "consume_queue_sufix": "q4_english_reviews",
        "publish_queue": "q4_english_review_count",
    }
    generate_counters_by_app_id(
        Q4_AMOUNT_OF_COUNTERS_BY_APP_ID, **q4_english_reviews_counter_args
    )

    q4_filter_more_than_5000_args = {
        "output": output,
        "query": "q4",
        "filter_name": "second_more_than_5000_",
        "input_queue_name": "q4_english_review_count",
        "output_queue_name": "q4_second_filter_more_than_5000",
        "amount_of_forwarding_queues": 1,
        "logging_level": "DEBUG",
        "column_number_to_use": 1,  # negative_english_review_count
        "value_to_filter_by": 5000,
        "criteria": "GREATER_THAN",
        "columns_to_keep": "0,1",  # app_id, positive_review_count
        "instances_of_myself": Q4_AMOUNT_OF_MORE_THAN_5000_FILTERS,
    }
    generate_filters_by_value(
        Q4_AMOUNT_OF_MORE_THAN_5000_FILTERS, **q4_filter_more_than_5000_args
    )

    add_join(
        output=output,
        query="q4",
        num=2,
        input_games_queue_name="1_q4_action_games",
        input_reviews_queue_name="0_q4_second_filter_more_than_5000",
        output_queue_name="Q4",
        amount_of_behind_nodes=1,  # 1 as the filters work as a group (will receive only one end from them)
        amount_of_forwarding_queues=1,  # 1 as the filters work as a group (will receive only one end from them)
    )


def generate_q5(output: Dict):
    q5_filter_action_games_args = {
        "output": output,
        "query": "q5",
        "filter_name": "action_games",
        "input_queue_name": "q5_games",
        "output_queue_name": "q5_shooter_games",
        "amount_of_forwarding_queues": 1,
        "logging_level": "INFO",
        "column_number_to_use": 2,  # genre
        "value_to_filter_by": "action",
        "criteria": "CONTAINS",
        "columns_to_keep": "0,1",  # app_id, positive_review_count
        "instances_of_myself": Q5_AMOUNT_OF_ACTION_GAMES_FILTERS,
    }
    generate_filters_by_value(
        Q5_AMOUNT_OF_ACTION_GAMES_FILTERS, **q5_filter_action_games_args
    )

    q5_filter_negative_reviews_args = {
        "output": output,
        "query": "q5",
        "filter_name": "negative_reviews",
        "input_queue_name": "q5_reviews",
        "output_queue_name": "q5_negative_reviews",
        "amount_of_forwarding_queues": Q5_AMOUNT_OF_COUNTERS_BY_APP_ID,
        "logging_level": "INFO",
        "column_number_to_use": 1,  # review_score
        "value_to_filter_by": -1.0,
        "criteria": "EQUAL_FLOAT",
        "columns_to_keep": "0",  # app_id, positive_review_count
        "instances_of_myself": Q5_AMOUNT_OF_NEGATIVE_REVIEWS_FILTERS,
    }
    generate_filters_by_value(
        Q5_AMOUNT_OF_NEGATIVE_REVIEWS_FILTERS, **q5_filter_negative_reviews_args
    )

    q5_negative_reviews_counter_args = {
        "output": output,
        "query": "q5",
        "consume_queue_sufix": "q5_negative_reviews",
        "publish_queue": "0_q5_counter",
    }

    generate_counters_by_app_id(
        Q5_AMOUNT_OF_COUNTERS_BY_APP_ID, **q5_negative_reviews_counter_args
    )

    add_join(
        output=output,
        query="q5",
        num=0,
        input_games_queue_name="0_q5_shooter_games",
        input_reviews_queue_name="0_q5_counter",
        output_queue_name="q5_percentile",
        amount_of_behind_nodes=Q5_AMOUNT_OF_COUNTERS_BY_APP_ID,
        amount_of_forwarding_queues=1,
    )

    add_percentile(
        output=output,
        query="q5",
        num=0,
        consume_queue="0_q5_percentile",  # TODO: Change if scalation of this node is implemented
        publish_queue="Q5",
    )


def generate_output():
    output = {}

    output["name"] = "steam_reviews_system"

    output["services"] = {}
    add_rabbit(output)
    add_client(output)
    generate_drop_columns(output, AMOUNT_OF_DROP_FILTER_COLUMNS)
    generate_drop_nulls(output, AMOUNT_OF_DROP_NULLS)

    # -------------------------------------------- Q1 -----------------------------------------
    # generate_q1(output=output)
    # # -------------------------------------------- Q2 -----------------------------------------
    # generate_q2(output=output)
    # # -------------------------------------------- Q3 -----------------------------------------
    # generate_q3(output=output)
    # -------------------------------------------- Q4 -----------------------------------------
    generate_q4(output=output)
    # -------------------------------------------- Q5 -----------------------------------------
    # generate_q5(output=output)
    # -------------------------------------------- END OF QUERIES -----------------------------------------

    add_volumes(output=output)

    output["networks"] = {}
    add_networks(output["networks"])

    return output


def main():
    output_file_name = sys.argv[1]
    # Recive the rest of the needed config params

    output = generate_output()

    create_file(output, output_file_name)

    print(f"{output_file_name} was successfully created")


main()
