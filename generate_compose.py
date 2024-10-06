import yaml
from typing import * 

def create_file(output, file_name):
  with open(file_name, 'w') as output_file:
    yaml.safe_dump(output, output_file, sort_keys=False, default_flow_style=False)

def add_networks(networks: Dict):
    networks["testing_net"] = {}
    networks["testing_net"]["ipam"] = {}
    networks["testing_net"]["ipam"]["driver"] = "default"
    networks["testing_net"]["ipam"]["config"] = [{"subnet": "172.25.125.0/24"}]

def generate_output():
    output = {}

    output["name"] = "steam_reviews_system"

    output["services"] = {}
    # TODO: Add the corresponding services

    output["networks"] = {}
    add_networks(output["networks"])

    return output

def main(args):
    output_file_name = args[1]
    # Recive the rest of the needed config params

    output = generate_output()

    create_file(output, output_file_name)

    print(f"{output_file_name} was successfully created")

main()