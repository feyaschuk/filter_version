import io
import re
import time

import schedule
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

token_credential = ClientSecretCredential("{tenant-id}", "{client-id}", "{client-secret}")

OAUTH_STORAGE_ACCOUNT_NAME = "prodeastus2data"
oauth_url = f"https://{OAUTH_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
blob_service_client = BlobServiceClient(account_url=oauth_url, credential=token_credential)

COUNT = 0
COUNT_IF_EXIST = 0

container_name = 'esg-dropdir'
folder = 'sustainalytics'
path_to_deliver = "dropdir/morningstar/sustainalytics/"
regex = 'raw/' + folder + '/' "[a-zA-Z_]+_v[0-9]+?_[0-9]{8}.txt"


def get_blob_list():
    '''Extracts list of files named with a list of the specific prefix.'''

    dr_dir = blob_service_client.get_container_client(container_name)
    blob_list = [x for x in dr_dir.list_blobs(
        name_starts_with="raw/" + folder + "/") if '/test.txt' not in x.name]
    if len(blob_list) == 0:
        print('No files with names start with specified prefix')
    return blob_list


def check_if_version(blob_list, regex):
    '''Checks if filename has a version in it.'''

    no_version = []
    with_version = []
    regex = re.compile(regex)
    for blob in blob_list:
        if regex.match(blob.name):
            with_version.append(blob.name)
        else:
            no_version.append(blob.name)
    return no_version, with_version


def filter_version(no_version, with_version):
    '''Filters files by version, removes from the result list old version files.'''

    old_version_list = []
    sorted_blob_list = sorted(with_version,
                              key=lambda x: (x).split('_').pop(0)+(x).split('_').pop())
    for prefix in prefix_list:
        res2 = [sub for sub in sorted_blob_list if prefix in sub]
        for i in range(1, len(res2)):
            datever_pr = ''.join(filter(lambda i: i.isdigit(), res2[i-1]))
            datever = ''.join(filter(lambda i: i.isdigit(), res2[i]))
            date_pr = datever_pr[1:]
            date = datever[1:]
            if date_pr == date:
                to_delete = (min(res2[i-1], res2[i]))
                old_version_list.append(to_delete)
    res = [i for i in with_version if i not in old_version_list]
    total_res = res + no_version
    return total_res


def upload(blob_list, COUNT, COUNT_IF_EXIST, total_res):
    '''Upload files to the destination folder. For the files with several
       versions upload last version file.'''
    '''Delete files from "raw" folder after processing.'''

    dr_dir = blob_service_client.get_container_client(container_name)
    print('start')
    for blob in blob_list:
        blob_name = blob.name
        print(f"{blob_name} file found and loading process started.")
        blob = blob_service_client.get_blob_client(container_name, blob_name)
        with io.BytesIO():
            download_stream = blob.download_blob(0)
            if blob_name in total_res:
                if dr_dir.get_blob_client(path_to_deliver + blob_name[4:]).exists():
                    COUNT_IF_EXIST += 1
                    print(f"{blob_name[19:]} already exists")
                elif dr_dir.get_blob_client(
                                            path_to_deliver + blob_name[4:]).upload_blob(
                                            download_stream.read(), blob_type="BlockBlob"):
                    print(f"{blob_name[19:]} uploaded successfully")
                    COUNT += 1
            else:
                print('Old version of the file, not for upload')
            blob.delete_blob()
        print(f"Blob deleted successfully from the raw!")
    print(f"{COUNT} file(s) uploaded successfully. "
          f"{COUNT_IF_EXIST} file(s) already existed in esg-dropdir/{path_to_deliver}.")


def main():

    blob_list = get_blob_list()    
    no_version, with_version = check_if_version(blob_list, regex)
    total_res = filter_version(no_version, with_version)
    upload(blob_list, COUNT, COUNT_IF_EXIST, total_res)


def task():
    '''Checks if the loading process is over, by counting files in "raw" folder.'''

    blob_list = get_blob_list()    
    time.sleep(5400)
    blob_list2 = get_blob_list()    
    if len(blob_list) == len(blob_list2):
        main()
    else:
        print('Loading process is not over yet.')


schedule.every(1).minutes.do(task)
# schedule.every().tuesday.at("18:00").do(task)


if __name__ == "__main__":

    prefix_list = ['ESGRR_Focus_', 'ESGRR_Indicators_',
                   'ESGRR_MEIs_', 'Ref_Data_', 'Carbon_Emissions_']
    while True:
        schedule.run_pending()
        time.sleep(1)
