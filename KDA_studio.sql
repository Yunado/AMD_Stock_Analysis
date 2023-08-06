%flink.ssql(type=update)

DROP TABLE IF EXISTS stock_table;

-- create a table to glue data catalog table with columns for stock data, 
-- sets a watermark to trigger late arrival events, and configures it to read 
-- from a Kinesis stream, in JSON format with an ISO-8601 timestamp format.
CREATE TABLE stock_table(
    `date` STRING,
    ticker VARCHAR(6),
    open_price FLOAT,
    high FLOAT,
    low FLOAT,
    close_price FLOAT,
    adjclose FLOAT,
    volume BIGINT,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time as event_time - INTERVAL '5' SECOND
) WITH ( --connect to your kinesis data stream
    'connector' = 'kinesis', 
    'stream' = 'mp11v2_ds',
    'aws.region' = 'us-east-1',
    'scan.stream.initpos' = 'TRIM_HORIZON',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);