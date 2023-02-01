import json
import os

import PartitionConsts as PC

PROMPT = "\nSelect a query: \n" + \
         "\t[0]: Get Github Event Log By Event Id\n" + \
         "\t[1]: Get Range of Github Event Log\n" + \
         "\t[2]: Get Github Event Log Summary\n" + \
         "\t[3]: Get All Repositories by Actor\n" + \
         "\t[4]: Get All Actors by Repository\n" + \
         "\t[5]: Quit\n"

QUERY_TRACKER = [0, 0]


# Make this prompt ask for a query type and input
def run_query_job(config):
    while True:
        keyboard_input = input(PROMPT)
        try:
            menu_choice = int(keyboard_input)
            if menu_choice == 0:
                successful = False
                while not successful:
                    keyboard_input = input("Enter a Github Event Log Id:\n")
                    try:
                        get_ghe_by_id(int(keyboard_input))
                        print_query_tracker_results()
                        successful = True
                    except:
                        print("Please enter a valid id.\n")
            elif menu_choice == 1:
                successful = False
                while not successful:
                    keyboard_input = input("Enter Github Event Log Id of Range Start:\n")
                    try:
                        start = int(keyboard_input)
                        while True:
                            keyboard_input = input("Enter Github Event Log Id of Range End:\n")
                            try:
                                end = int(keyboard_input)
                                if start > end:
                                    print("Start > End, Please enter a valid Range.\n")
                                else:
                                    get_ghe_by_range(start, end)
                                    print_query_tracker_results()
                                    successful = True
                                break
                            except:
                                print("Please enter a valid End Id\n")
                    except:
                        print("Please enter a valid Start Id\n")

            elif menu_choice == 2:
                get_ghe_event_type_summary()
            elif menu_choice == 3:
                keyboard_input = input("Enter an Actor display_login: \n")
                get_actor_repository_summary(keyboard_input)
                print_query_tracker_results()
            elif menu_choice == 4:
                keyboard_input = input("Enter an Repository name: \n")
                get_repository_actor_summary(keyboard_input, config[3][3])
                print_query_tracker_results()
            elif menu_choice == 5:
                print("Quitting...")
                break
            else:
                print("Please enter a valid menu choice.\n")
        except:
            print("Please enter a valid menu choice.\n")


def print_query_tracker_results():
    global QUERY_TRACKER
    print("\nPartitions Opened: {0}".format(QUERY_TRACKER[0]))
    print("Lines Parsed: {0}\n".format(QUERY_TRACKER[1]))
    QUERY_TRACKER = [0, 0]


def get_ghe_by_id(target_ghe_id):
    print("Beginning Query...")
    global QUERY_TRACKER
    target_partition_no = find_ghe_target_partition(target_ghe_id)
    try:
        with open("{0}/".format(PC.PARTITIONS_DIRECTORY) + PC.PARTITION_BASE_NAME.format(str(target_partition_no)), 'r',
                  encoding='utf-8') as target_partition:
            QUERY_TRACKER[0] += 1
            for ghe_log in target_partition:
                QUERY_TRACKER[1] += 1
                if int(json.loads(ghe_log)["id"]) == target_ghe_id:
                    print("Found target: \n" + ghe_log)
                    return
        print("Event {0} does not exist.".format(target_ghe_id))
    except Exception as e:
        print("Failed to load partition " + str(e))


def get_ghe_by_range(start_ghe_id, end_ghe_id):
    print("Beginning Query...")
    global QUERY_TRACKER
    starting_partition_no = find_ghe_target_partition(start_ghe_id)
    ending_partition_no = find_ghe_target_partition(end_ghe_id)
    try:
        for partition_no in range(starting_partition_no,
                                  ending_partition_no
                                  if ending_partition_no != starting_partition_no
                                  else (ending_partition_no + 1)):
            with open("{0}/".format(PC.PARTITIONS_DIRECTORY) + PC.PARTITION_BASE_NAME.format(str(partition_no)), 'r',
                      encoding='utf-8') as target_partition:
                QUERY_TRACKER[0] += 1
                for ghe_log in target_partition:
                    QUERY_TRACKER[1] += 1
                    if start_ghe_id <= int(json.loads(ghe_log)["id"]) <= end_ghe_id:
                        print(ghe_log)
    except Exception as e:
        print("Failed to open partitions " + str(e))


def get_ghe_event_type_summary():
    print("Beginning Query...")
    try:
        print("Utilizes Index File...")
        with open(PC.INDEX_FILE_NAME, 'r', encoding='utf-8') as index_file:
            index_document = json.loads(index_file.read())
            for key in index_document[PC.IRM_EVENT_TYPE_TRACKER]:
                print(key + ": " + str(index_document[PC.IRM_EVENT_TYPE_TRACKER][key]))
    except Exception as e:
        print("Failed to load index file: " + str(e))


def get_actor_repository_summary(target_actor):
    print("Beginning Query...")
    global QUERY_TRACKER
    try:
        with open(PC.INDEX_FILE_NAME, 'r', encoding='utf-8') as index_file:
            index_document = json.loads(index_file.read())
            try:
                actor_repository_index_set = index_document[PC.IRM_AUTHORS_INDEX_DICTIONARY][target_actor]
                if actor_repository_index_set or actor_repository_index_set == 0:
                    actor_repository_set = set()
                    for partition_no in actor_repository_index_set:
                        with open("{0}/".format(PC.PARTITIONS_DIRECTORY) + PC.PARTITION_BASE_NAME.format(
                                str(partition_no)),
                                  'r',
                                  encoding='utf-8') as target_partition:
                            QUERY_TRACKER[0] += 1
                            for ghe_log in target_partition:
                                QUERY_TRACKER[1] += 1
                                current_log = json.loads(ghe_log)
                                if current_log["actor"]["display_login"] == target_actor:
                                    actor_repository_set.add(current_log["repo"]["name"])

                    print("Actor {0} has accessed the following repositories: ".format(target_actor))
                    for repo_name in actor_repository_set:
                        print(repo_name)
            except Exception as a:
                print("Actor {0} does not exist in dataset.".format(target_actor) + str(a))
    except Exception as e:
        print("Failed to load index file: " + str(e))


def get_repository_actor_summary(target_repository, num_partitions):
    print("Beginning Query...")
    global QUERY_TRACKER
    try:
        repository_actor_set = set()
        for partition_no in range(num_partitions):
            with open("{0}/".format(PC.PARTITIONS_DIRECTORY) + PC.PARTITION_BASE_NAME.format(
                    str(partition_no)),
                      'r',
                      encoding='utf-8') as target_partition:
                QUERY_TRACKER[0] += 1
                for ghe_log in target_partition:
                    QUERY_TRACKER[1] += 1
                    current_log = json.loads(ghe_log)
                    if current_log["repo"]["name"] == target_repository:
                        repository_actor_set.add(current_log["actor"]["display_login"])

        print("Repository {0} has been accessed by the following authors: ".format(target_repository))
        for actor_name in repository_actor_set:
            print(actor_name)
    except Exception as a:
        print("Failed parsing partitions: " + str(a))


# Helper Function
def find_ghe_target_partition(target_ghe_id):
    try:
        with open(PC.INDEX_FILE_NAME, 'r', encoding='utf-8') as index_file:
            index_document = json.loads(index_file.read())
        if index_document:
            event_ranges = index_document[PC.IRM_EVENT_RANGE_INDEX]
            if not (target_ghe_id > max(event_ranges) or target_ghe_id < min(event_ranges)):
                for i in range(0, len(event_ranges), 2):
                    start, end = event_ranges[i], event_ranges[i + 1]
                    if start <= target_ghe_id <= end:
                        return int(i / 2)
    except Exception as e:
        print("Failed to load index file: " + str(e))
    return -1




