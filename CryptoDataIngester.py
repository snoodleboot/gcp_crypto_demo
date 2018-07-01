import datetime, time, traceback, urllib.parse
import pandas as pd
from pandas.io.json import json_normalize
from requests import get

from BucketFileWriter import BucketFileWriter
from ConfigReader import ConfigReader
from CryptoIngestResponseError import CryptoIngestResponseError
from CryptoIngesterError import CryptoIngesterError


class CryptoDataIngester:
    """
    This class queries a crypto currency rate source. It will update it at most once every 5 minutes (as that is the
    rate at which the data is updated). The data once (and as) it is queried, is persisted to a data store through a
    data interactor, or to multiple locations through a collection of data interactors. This is not the most performant
    approach, but extra effort was given to make sure that the API's robot rules are obeyed.

    Numerous embedded (scoped) classes were used to control their use to this classes (intent).

    API called has docs here: https://coinmarketcap.com/api/
    """


    class InitializedStruct:

        @property
        def first_ticker_added(self):
            return self._first_ticker_added

        @first_ticker_added.setter
        def first_ticker_added(self,value):
            if isinstance(value, bool):
                self._first_ticker_added = value
            else:
                raise CryptoIngesterError(
                    'InitializedStruct.first_ticker_added must be set as a bool, not as a {}'.format(type(value)))

        @property
        def initialized(self):
            return self.listing_initialized and self.tickers_initialized and self.global_content_initialized

        @property
        def listing_initialized(self):
            return self._listing_initialized

        @listing_initialized.setter
        def listing_initialized(self, value):
            if isinstance(value, bool):
                self._listing_initialized = value
            else:
                raise CryptoIngesterError(
                    'InitializedStruct.listing_initialized must be set as a bool, not as a {}'.format(type(value)))

        @property
        def tickers_initialized(self):
            return self._tickers_initialized

        @tickers_initialized.setter
        def tickers_initialized(self, value):
            if isinstance(value, bool):
                self._tickers_initialized = value
            else:
                raise CryptoIngesterError(
                    'InitializedStruct.tickers_initialized must be set as a bool, not as a {}'.format(type(value)))

        @property
        def global_content_initialized(self):
            return self._global_content_initialized

        @global_content_initialized.setter
        def global_content_initialized(self, value):
            if isinstance(value, bool):
                self._global_content_initialized = value
            else:
                raise CryptoIngesterError(
                    'InitializedStruct.global_content_initialized must be set as a bool, not as a {}'.format(type(value)))

        def __init__(self):
            self.listing_initialized = False
            self.tickers_initialized = False
            self.global_content_initialized = False
            self.first_ticker_added = False

    class PageTracker:
        def __init__(self, page_length, max_page):
            self._page_length = page_length
            self._max_page = max_page

            self._next_page = None
            self._completed_full_cycle = False
            self._last_page = None
            self._comping_page = None

        @property
        def last_page(self):
            return self._last_page

        @property
        def max_page(self):
            return self._max_page

        @max_page.setter
        def max_page(self,value):
            self._max_page = value

        @property
        def completed_full_cycle(self):
            return self._completed_full_cycle

        @property
        def next_page(self):

            if self._next_page is None:
                page_to_return = 1
                self._last_page = page_to_return
                self._next_page = self._page_length + 1
            else:
                page_to_return = self._next_page
                self._last_page = page_to_return
                self._next_page = self._next_page + self._page_length
                if self._next_page > self.max_page:
                    self._next_page = 1
                    self._completed_full_cycle = True
                pass

            return page_to_return

        @property
        def coming_page(self):
            return self._next_page

    class CSV_Persistor:

        def __init__(self,bucket):
            self.bucket_writer = BucketFileWriter(bucket_name=bucket)

        def persist(self,blob_filename,dataframe):
            if not isinstance(dataframe,pd.DataFrame):
                raise ValueError('dataframe must be of type pandas.DataFrame')

            import os
            print(dataframe.columns)
            dataframe.to_csv(blob_filename, index = False)

            self.bucket_writer.upload(blob_filename)

    class Logger: #TODO This is blocking and simple. Change to operate on thread and write to BigTable, Mongo, or other noSQL DB at a later time

        error_log = 'errors.log'
        api_errors = 'api_errors.log'

        def __init__(self,bucket):
            self.bucket_writer = BucketFileWriter(bucket_name=bucket)

        def log(self,message,type_of_message):
            if type_of_message=='error':
                with open(self.error_log,'a') as fh:
                    fh.write(message)
                self.bucket_writer.upload(self.error_log)
            elif type_of_message=='api_error':
                with open(self.api_errors,'a') as fh:
                    fh.write(message)
                self.bucket_writer.upload(self.api_errors)
            else:
                pass

    def __init__(self):

        config_reader = ConfigReader()
        config = config_reader.config

        self._listing_config = config['api']['listings']
        self._ticker_config = config['api']['ticker']
        self._global_config = config['api']['global']
        self._persist_config = config['persist']

        self.pull_frequency_minimum_interval = config['api']['pull_frequency_minimum_interval']

        self._error_timeout = config['api']['error_timeout']
        self._timeout = config['api']['timeout']
        self._refresh_timeout = self._ticker_config['refresh_period']
        self._timeout_start = None
        self._in_timeout = False

        self._currency_df = pd.DataFrame.empty

        self._meta_data = None

        self._last_pull = self.current_time

        self._last_listing_update = self.current_time
        self._initilization_tracker = self.InitializedStruct()

        self.page_tracker = self.PageTracker(page_length=config['api']['ticker']['page_length'],
                                             max_page=None)

        self._persistor = self.CSV_Persistor(bucket=self._persist_config['bucket'])
        self._logger = self.Logger(bucket=self._persist_config['bucket'])

        self.running = False

    @property
    def current_time(self):
        return datetime.datetime.now()

    @property
    def pull_allowed(self):
        condition_1 = (self.current_time - self._last_pull).total_seconds() > self.pull_frequency_minimum_interval
        condition_2 = self._timeout_start is None or ((self.current_time - self._timeout_start).total_seconds() > self._refresh_timeout)

        self._in_timeout = not condition_2

        return condition_1 and condition_2

    def run_updater(self):
        """
        This is the engine. It will make sure everything is initialized (data) and that it keep up to date. Any handlable
        exception will be handled and logged. This software should only shutdown on KeyboardInterrupt.
        """

        self.running = True

        while(self.running):

            try:
                if self._initilization_tracker.initialized:
                    self._update_data()
                else:
                    self._initialize_data()
                time.sleep(self._timeout)

            except KeyboardInterrupt:
                print('exiting software')
                msg = json.dumps({'message':'Exiting software by Keyboard Interrupt.','time':self.current_time.strftime('%s')})
                self._logger.log(message=msg,
                                 type_of_message='error')
                self.running = False

            except CryptoIngestResponseError as ex:
                self._logger.log(message=str(ex),type_of_message='api_error')
                print('Encountered API error. Sleeping for {0} seconds.'.format(self._error_timeout))
                time.sleep(self._error_timeout)

            except:

                print(traceback.format_exc())
                msg = json.dumps({'message':traceback.format_exc(),'time':self.current_time.strftime('%s')})
                self._logger.log(message=msg,
                                 type_of_message='error')
                print('Encountered error. Sleeping for {0} seconds.'.format(self._error_timeout))
                time.sleep(self._error_timeout)

    def _initialize_data(self):

        if not self._initilization_tracker.global_content_initialized:
            if self._update_global(override_update=True):
                self._initilization_tracker.global_content_initialized = True
        elif not self._initilization_tracker.listing_initialized:
            if self._update_currency_list(override_update=True):
              self._initilization_tracker.listing_initialized = True
        elif not self._initilization_tracker.tickers_initialized:
            self._update_next_ticker_set()
            if self.page_tracker.completed_full_cycle:
                self._initilization_tracker.tickers_initialized = True

    def _update_data(self):
        self._update_currency_list()

        self._update_next_ticker_set()

    def _update_global(self, override_update = False):

        try:

            time = self.current_time

            if ((time - self._last_listing_update).total_seconds() > self._global_config['pull_frequency'] or override_update) \
                    and self.pull_allowed:
                result = get(self._global_config['address']).json()
                self._last_pull = time
                if result['data']:
                    self.page_tracker.max_page = result['data']['active_cryptocurrencies']
                else:
                    raise CryptoIngestResponseError(json.dumps(result['metadata']))
                return True
            else:
                return False

        except:
            print(traceback.format_exc())
            raise

    def _update_next_ticker_set(self):
        try:

            if (self.pull_allowed):

                time = self.current_time

                query = '?start={0}&sort=id&structure=array'.format(self.page_tracker.next_page)
                get_url = urllib.parse.urljoin(self._ticker_config['address'],query)

                result = get(get_url).json()
                self._last_pull = time

                if result['data']:
                    records = json_normalize(result['data'])
                    response_df = pd.DataFrame.from_records(records,index='id')
                else:
                    raise CryptoIngestResponseError(json.dumps(result['metadata']))

                if self._initilization_tracker.first_ticker_added:
                    self._currency_df.update(response_df)
                else:
                    self._currency_df = pd.merge(left=self._currency_df, right=response_df, on=['name', 'symbol', 'website_slug'], how='left')
                    self._initilization_tracker.first_ticker_added = True

        except:
            print(traceback.format_exc())
            raise

        finally:
            if self.page_tracker.coming_page == 1 and not self._in_timeout:
                self._timeout_start = self.current_time
                self._persistor.persist(blob_filename=self._persist_config['filename'],
                                        dataframe=self._currency_df)
                self._in_timeout = True

    def _update_currency_list(self, override_update = False):
        try:

            time = self.current_time

            if ((time - self._last_listing_update).total_seconds() > self._listing_config['pull_frequency'] or override_update)\
                    and self.pull_allowed:

                result = get(self._listing_config['address']).json()
                self._last_pull = time
                if result['data']:
                    response_df = pd.DataFrame.from_records( result['data'], index='id' )
                else:
                    raise CryptoIngestResponseError(json.dumps(result['metadata']))

                if self._currency_df == pd.DataFrame.empty:
                    self._currency_df = response_df
                else:
                    self._currency_df = pd.merge(left=self._currency_df, right=result, on=['name', 'symbol', 'website_slug'], how='right')

                return True
            else:
                return False

        except:
            print(traceback.format_exc())
            raise


if __name__ == '__main__':
    di = CryptoDataIngester()

    import json

    di.run_updater()