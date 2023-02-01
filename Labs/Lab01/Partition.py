import json
import shutil
import os
import traceback

import PartitionConsts as PC


def create_partitions_directory(partition_directory_name):
    try:
        if os.path.exists(os.path.join(os.getcwd(), partition_directory_name)):
            keyboard_input = input("\033[93m Attempting to delete directory: {0}\{1}\033[0m\n".format(os.getcwd(),partition_directory_name) +
                  "Type \'Allow\' to proceed: \n")
            if keyboard_input == "Allow":
                shutil.rmtree(os.path.join(os.getcwd(), partition_directory_name))
        print("Creating {0} directory...\n".format(partition_directory_name))
        os.mkdir(partition_directory_name)
    except Exception as e:
        print("Failed in making partition directory:" + str(e))


# For every object in the JSON_EVENT_FILE, run the partition function on it.
def partition_bulk_data(json_bulk_data_file_name, partition_config):
    try:
        with open(json_bulk_data_file_name, 'r', encoding='utf-8') as ghe_data:
            partition_strategy(ghe_data, partition_config)
    except Exception as e:
        print('Failed to import data: ' + str(e))


def partition_strategy(ghe_data, partition_config):
    partitioning_strategy = partition_config[4]

    if partitioning_strategy == PC.PartitioningStrategy.RANDOM:
        random_partition_method(ghe_data, partition_config[0],
                                partition_config[1],
                                partition_config[2],
                                partition_config[3])
    elif partitioning_strategy == PC.PartitioningStrategy.RANGE:
        range_partition_method(ghe_data, partition_config[0],
                               partition_config[1],
                               partition_config[2],
                               partition_config[3])
    elif partitioning_strategy == PC.PartitioningStrategy.HASH:
        hash_partition_method(ghe_data, partition_config[0],
                              partition_config[1],
                              partition_config[2],
                              partition_config[3])
    elif partitioning_strategy == PC.PartitioningStrategy.INTERLEAVED:
        interleaved_partition_method(ghe_data,
                                     partition_config[0],
                                     partition_config[1],
                                     partition_config[2],
                                     partition_config[3])


def random_partition_method(ghe_data, index_file_name, partition_directory_name, partition_base_name, num_partitions):
    pass


def hash_partition_method(ghe_data, index_file_name, partition_directory_name, partition_base_name, num_partitions):
    pass


def interleaved_partition_method(ghe_data, index_file_name, partition_directory_name, partition_base_name,
                                 num_partitions):
    pass


# Helper function to deal with creating dictionary entries
def dictionary_put(dictionary, key, partition_no):
    if key in dictionary.keys():
        if partition_no not in dictionary[key]:
            dictionary[key].append(partition_no)
    else:
        dictionary[key] = [partition_no]


# Only tracking one extra attribute apart from the event range (Authors)
def range_partition_method(ghe_data, index_file_name, partition_directory_name, partition_base_name, num_partitions):
    try:
        ghe_lines = ghe_data.readlines()
        tenth_of_lines = int(len(ghe_lines)/num_partitions)

        event_range_index = []
        event_type_tracker = {}
        authors_index_dictionary = {}
        # repository_index_dictionary = {}

        for i in range(num_partitions):
            # Create a partition and write every log necessary
            try:
                with open("{0}/".format(partition_directory_name) + partition_base_name.format(str(i)), 'w',
                          encoding='utf-8') as current_partition:
                    start = i * tenth_of_lines
                    end = (i + 1) * tenth_of_lines if (i + 1) != num_partitions else len(ghe_lines)
                    print("Partitioning Event Logs: " + str(start) + " " + str(end))
                    for j in range(start, end):
                        current_ghe_log = json.loads(ghe_lines[j])

                        # Track a 2D array with [Event_first, Event_last] for every partition
                        if j == start or j == (end - 1):
                            event_range_index.append(int(current_ghe_log["id"]))

                        # Perform a dictionary put on (ActorName -> PartitionNo)
                        author_key = current_ghe_log["actor"]["display_login"]
                        dictionary_put(authors_index_dictionary, author_key, i)

                        # Tracks total number of events as a dictionary with KVP (EventType -> Count)
                        if current_ghe_log["type"] in event_type_tracker.keys():
                            event_type_tracker[current_ghe_log["type"]] += 1
                        else:
                            event_type_tracker[current_ghe_log["type"]] = 1

                        # repo_key = current_ghe_log["repo"]["name"]
                        # dictionary_put(repository_index_dictionary, repo_key, i)

                        current_partition.write(ghe_lines[j])
            except Exception as p:
                traceback.print_exc()

            try:
                with open(index_file_name, 'w', encoding='utf-8') as index_file:
                    aggregate_dict = {
                        PC.IRM_EVENT_RANGE_INDEX: event_range_index,
                        PC.IRM_EVENT_TYPE_TRACKER: event_type_tracker,
                        PC.IRM_AUTHORS_INDEX_DICTIONARY: authors_index_dictionary,
                        # PC.IRM_REPO_INDEX_DICTIONARY: repository_index_dictionary
                    }
                    json.dump(aggregate_dict, index_file)
            except Exception as i:
                print("Failed to write index file: " + str(i))

    except Exception as e:
        print("Failed to read input data: " + str(e))


def run_partition_job(config):
    if config:
        create_partitions_directory(config[3][1])
        partition_bulk_data(config[2], config[3])
    else:
        print("Failed to run partition job, missing config.")
