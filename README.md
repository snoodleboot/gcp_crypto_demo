#GCP Challenge - John Aven
---

## About
The purpose of the challenge was to do the following:
1. Get data from cryptocurrency API
2. Put data in csv
3. load csv to google bucket
4. load csv from bucket into bigquery
5. perform queries on bigquery via datalabgm

...


## Solution

The solution consists of two parts. The front-end (queries) and the back-end (everything else)

### Front-End
I will discuss the front-end first. In the front-end, then aim is to provide answers to data insight requests. These request are pre-canned and can be expressed in SQL queries against BigQuery. This is all straightforward in the notebooks in DataLab. 

### Back-nd
The back-end is more involved (see code), but can be expressed simply. First, the API has robot-rules; no more than 30 queries per minute. To obey the rules and to be extra friendly, the queries happen in the neighborhood of 15 per minute. This slower than best performance is alright though. It avoids the risk of violating the restrictions and getting black-listed. In production this could be tuned by looking at the average roundtrip time and expected deviations from that rate. The code, as the nature of the data changes over time, should be able to keep the data ever-green. The code was thus written as a service (ideally hosted in a docker container - or similar) and scheduled to update at a rate of about once every 5-8 minutes. The code should not hard-code configuration, but should store it externally. The decision was to store this configuration in a YAML file in a google-bucket. This required writing a stream reader around the Google-API for buckets. Each of the tables (Listing, Global and Tickers) are queried separately (and tickers is done iteratively) and go into building a single, joined, DataFrame. The global data is only pulled once. For production I would add this in to pull periodically and update the total number of currencies. Errors are logged to JSON files (need some formatting still). So are event notices and errors from API queries.

To simplify the workflow and allow all of it to run within the notebook without any fancy threading work, I modified the code to have a forced interrupt after the first update to the data.

### Future Work
If I were to change anything, I would remove (or do in addition to) the writing to the CSV file in the google bucket, and use the API to write directly to the BigQuery, keeping the data truly evergreen. In fact, this could be done on pull and not written to internal DB (pandas DataFrame as in memory DB). 

Cheers

### Util

All code which is not in the runtime will
