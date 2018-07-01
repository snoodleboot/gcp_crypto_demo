import io
import traceback

from google.cloud import storage


class BucketFileStreamReader:

    def __init__(self,bucket_name):

        self._bucket_name = bucket_name

    def read(self,filename):

        # get client connection
        client = storage.Client()

        try:
            # get handle to the bucket
            bucket = client.get_bucket(self._bucket_name)
            # get the blob from the bucket
            blob = bucket.get_blob(filename)

            # Return the file as a stream to the caller
            return io.BytesIO(blob.download_as_string())

        except:
            print(traceback.format_exc())
            raise

