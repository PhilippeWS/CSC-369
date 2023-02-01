import requests
import gzip
import shutil


def get_ghe_gz_file(request_url, gz_ghe_event_file_name):
    try:
        gz_file_request = requests.get(request_url)
        try:
            with open(gz_ghe_event_file_name, 'wb') as compressed_file:
                compressed_file.write(gz_file_request.content)
                return True
        except Exception as c:
            print("Failed to write compressed file: " + str(c))
    except Exception as e:
        print("Failed request to GH Archive: " + str(e))
    return False


def unzip_gz_file(gz_ghe_event_file_name, json_bulk_data_file_name):
    try:
        with gzip.open(gz_ghe_event_file_name, 'rb') as f_in:
            with open(json_bulk_data_file_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return True
    except Exception as e:
        print("Failed to unzip gz file" + str(e))
        return False

def run_request_job(config):
    if get_ghe_gz_file(config[0], config[1]):
        unzip_gz_file(config[1], config[2])
