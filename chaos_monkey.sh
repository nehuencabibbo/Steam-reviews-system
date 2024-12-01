#!/bin/bash

# Assign the first argument to sleep_time
readonly SLEEP_TIME_AFTER_STOP=0.2
readonly SLEEP_TIME_AFTER_START=1.2

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
    if [[ ! $name =~ client ]] && [[ ! $name =~ rabbit ]] && [[ ! $name =~ join ]] && [[ ! $name =~ percentile ]]; then
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
    docker kill "$container_to_stop"
    echo "Stopping container: $container_to_stop, sleeping for: $SLEEP_TIME_AFTER_STOP seconds"
    sleep "$SLEEP_TIME_AFTER_STOP"
    docker start "$container_to_stop"
    echo "Re-starting container: $container_to_stop, sleeping for: $SLEEP_TIME_AFTER_START seconds"
    sleep "$SLEEP_TIME_AFTER_START"
done