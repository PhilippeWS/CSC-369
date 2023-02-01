import Request
from Request import run_request_job
from Partition import run_partition_job
from Query import run_query_job
import PartitionConsts as PC
import datetime
import re
import os

# Config, a multidimensional array that contains all names and numbers needed to perform requests and partitioning
# Config[0]: Request URL for the GH Archive API
# Config[1]: Name of the .json.gz file that contains the bulk data needing to be unzipped
# Config[2]: Name the file to write the bulk data to.
# Config[3]: An array of configurations needed to partition
    # Config[3][0]: Name of the index file where the index information is to be written
    # Config[3][1]: Name of the partition directory where all partitions are kept
    # Config[3][2]: Base name of a partition file. Each will be enumerated based on Config[3][3]
    # Config[3][3]: Number of partitions to create for a partitioning strategy
    # Config[3][4]: The Partitioning strategy to employ
if __name__ == '__main__':
    # Defaults
    partition_config = [PC.GITHUB_EVENTS_REQUEST_URL.format(PC.DEFAULT_REQUEST_PARAM),
                                PC.GZ_EVENT_FILE_NAME.format(PC.DEFAULT_REQUEST_PARAM),
                                PC.JSON_BULK_DATA_FILE_NAME.format(PC.DEFAULT_REQUEST_PARAM),
                                [PC.INDEX_FILE_NAME,
                                 PC.PARTITIONS_DIRECTORY,
                                 PC.PARTITION_BASE_NAME,
                                 PC.NUM_PARTITIONS,
                                 PC.PartitioningStrategy.RANGE]]

    keyboard_input = input("Run partition job (Y/N)?\n")
    if re.search('y', keyboard_input, re.IGNORECASE):
        keyboard_input = input("Customize partition job (Y/N)?\n")
        if re.search('y', keyboard_input, re.IGNORECASE):
            partition_config = []
            while True:
                keyboard_input = input("Enter target GH Archive date in YYYY-MM-DD format (or \'default\'):\n")
                try:
                    if re.search('default', keyboard_input, re.IGNORECASE):
                        print("Applying default date and hour...")
                        partition_config.append(PC.GITHUB_EVENTS_REQUEST_URL.format(PC.DEFAULT_REQUEST_PARAM))
                        partition_config.append(PC.GZ_EVENT_FILE_NAME.format(PC.DEFAULT_REQUEST_PARAM))
                        partition_config.append(PC.JSON_BULK_DATA_FILE_NAME.format(PC.DEFAULT_REQUEST_PARAM))
                        partition_config.append([])
                    else:
                        datetime.datetime.strptime(keyboard_input, '%Y-%m-%d')
                    break
                except ValueError:
                    print("Incorrect date format.")

            while True:
                if re.search('default', keyboard_input, re.IGNORECASE):
                    break
                ghe_date = keyboard_input
                keyboard_input = input("Enter the hour (0-23) of the GH Archive logs to be requested: \n")
                try:
                    if 0 <= int(keyboard_input) <= 23:
                        ghe_date = ghe_date + "-" + keyboard_input
                        partition_config.append(PC.GITHUB_EVENTS_REQUEST_URL.format(ghe_date))
                        partition_config.append(PC.GZ_EVENT_FILE_NAME.format(ghe_date))
                        partition_config.append(PC.JSON_BULK_DATA_FILE_NAME.format(ghe_date))
                        partition_config.append([])
                        break
                except Exception as e:
                    print("Invalid hour entered.")

            keyboard_input = input("Enter and index file name (or \'default\'):\n")
            if re.search('default', keyboard_input, re.IGNORECASE):
                print("Applying default index file name...")
                keyboard_input = PC.INDEX_FILE_NAME
            partition_config[3].append(keyboard_input)

            keyboard_input = input("Enter partition directory name (or \'default\'):\n" +
                                   '\033[93m' +
                                   "WARNING: This will attempt to delete and remake the directory with the passed name:" +
                                   '\033[0m\n')
            if re.search('default', keyboard_input, re.IGNORECASE):
                print("Applying default partition directory name...")
                keyboard_input = PC.PARTITIONS_DIRECTORY
            partition_config[3].append(keyboard_input)

            partition_config[3].append(PC.PARTITION_BASE_NAME)

            while True:
                keyboard_input = input("Enter number of partitions (1-10) (or \'default\'):\n")
                try:
                    if re.search('default', keyboard_input, re.IGNORECASE):
                        print("Applying default number of partitions...\n")
                        keyboard_input = PC.NUM_PARTITIONS
                        break
                    elif 1 <= int(keyboard_input) <= 10:
                        break
                    else:
                        print("Please enter a valid number.")
                except Exception as e:
                    print("Please enter a valid number.")
            partition_config[3].append(keyboard_input)

            partition_config[3].append(PC.PartitioningStrategy.RANGE)
        else:
            print("Utilizing default partition config.\n")

        keyboard_input = input("Request data from GH Archive api (Y/N)?\n")
        if re.search('y', keyboard_input, re.IGNORECASE):
            print("\nRequesting and importing data from GH Archive api...\n")
            run_request_job(partition_config)
        print("Beginning partition job...\n")
        run_partition_job(partition_config)

    if len(os.listdir(os.path.join(os.getcwd(), partition_config[3][1]))) == partition_config[3][3]:
        run_query_job(partition_config)
    else:
        print("\033[91mPartition directory does not exist, cannot perform queries. Quitting...\033[0m")
