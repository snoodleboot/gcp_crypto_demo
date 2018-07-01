import traceback, yaml
from BucketFileStreamReader import BucketFileStreamReader


class ConfigReader:


    def __init__(self):

        self._bucket_file_reader = BucketFileStreamReader(bucket_name="gcp-challenge-javen-caserta")


    @property
    def config(self):
        #TODO: cache the config
        return self._read_config()

    def _read_config(self):

        with self._bucket_file_reader.read(filename="config.yml") as stream:
            try:
                return yaml.load(stream)
            except:
                print(traceback.format_exc())
                raise
