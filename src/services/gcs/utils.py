import os



def download_bucket_objects(bucket_name, blob_path, local_path):
    # blob path is bucket folder name
    print("-- Downloading GCS folder ...")
    command = "gsutil cp -q -r gs://{bucketname}/{blobpath} {localpath}".format(bucketname = bucket_name, blobpath = blob_path, localpath = local_path)
    os.system(command)
    return command