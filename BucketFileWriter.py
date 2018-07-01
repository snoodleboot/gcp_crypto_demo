import io
import traceback

from google.cloud import storage


class BucketFileWriter:

    def __init__(self,bucket_name):

        self._bucket_name = bucket_name

    def upload(self,filename):

        # get client connection
        client = storage.Client()

        try:
            # get handle to the bucket
            bucket = client.get_bucket(self._bucket_name)
            # get the blob from the bucket
            blob = bucket.blob(filename)
            blob.upload_from_filename(filename=filename)

        except:
            print(traceback.format_exc())
            raise

