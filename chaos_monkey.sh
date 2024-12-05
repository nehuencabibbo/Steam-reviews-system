#!/bin/bash

#Get failure rate
failure_rate=$1

# Assign the first argument to sleep_time
readonly SLEEP_TIME_AFTER_STOP=1.2


if [ -z "$1" ]; then
    echo "Usage: $0 <failure_rate>"
    exit 1
fi

while true; do
    container_names=$(docker ps --format '{{.Names}}')
    container_names_array=($container_names)

    for name in "${container_names_array[@]}"; do
        if [[ $name =~ rabbit ]]; then
            rabbit_found=true
            break
        fi
    done

    if $rabbit_found; then
        break
    fi

    echo "No container containing 'rabbit' found. Sleeping and Retrying..."
    sleep 0.5
done

# Filter out elements containing "client" or "rabbit"
filtered_container_names_array=()
for name in "${container_names_array[@]}"; do
    if [[ ! $name =~ client ]] && [[ ! $name =~ rabbit ]] ; then
        filtered_container_names_array+=("$name")
    fi
done

while true; do
    if [ ${#filtered_container_names_array[@]} -eq 0 ]; then
        echo "No containers to select from."
        continue
    fi

    random_index=$((RANDOM % ${#filtered_container_names_array[@]}))
    
    container_to_stop="${filtered_container_names_array[$random_index]}"
    
    # If there is only one monitor left, do not kill it. This is for demo
    if [[ $container_to_stop =~ watchdog ]]; then
        container_names=$(docker ps --format '{{.Names}}' | grep watchdog)
        container_names_array=($container_names)
        if [[ ${#container_names_array[@]} -lt 2 ]]; then
            sleep "$SLEEP_TIME_AFTER_STOP"
            continue
        fi
    fi

    #Get random number to check if the container should fail or not
    random_number=$(echo "scale=4; $RANDOM/32767" | bc)

    if awk -v num1="$failure_rate" -v num2="$random_number" 'BEGIN {exit !(num1 > num2)}'; then
        echo "Killing container: $container_to_stop"
        docker kill "$container_to_stop"
    fi

    sleep "$SLEEP_TIME_AFTER_STOP"

    
done