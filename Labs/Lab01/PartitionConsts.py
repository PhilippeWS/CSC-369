from enum import Enum

class PartitioningStrategy(Enum):
    RANDOM = 1
    RANGE = 2
    HASH = 3
    INTERLEAVED = 4


GITHUB_EVENTS_REQUEST_URL = "https://data.gharchive.org/{0}.json.gz"
GZ_EVENT_FILE_NAME = '{0}.json.gz'
JSON_BULK_DATA_FILE_NAME = '{0}_raw_ghe_data.json'
DEFAULT_REQUEST_PARAM = '2021-03-01-7'

INDEX_FILE_NAME = 'index.json'

PARTITIONS_DIRECTORY = 'partitions'
PARTITION_BASE_NAME = 'partition{0}.json'
NUM_PARTITIONS = 10

# Index Reference Manager
IRM_EVENT_RANGE_INDEX = "event_range_index"
IRM_EVENT_TYPE_TRACKER = "event_type_tracker"
IRM_AUTHORS_INDEX_DICTIONARY = "authors_index_dictionary"
IRM_REPO_INDEX_DICTIONARY = "repo_index_dictionary"
