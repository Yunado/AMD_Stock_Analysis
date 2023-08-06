# Streaming Data Analysis Workflow using AWS Kinesis AWS Data Analytics
AMD stock analysis using AWS Kinesis Data Analytics Studio

# Stock Price Data

This data contains historical stock price data for AMD from 2021-2022, organized in a csv format. The dataset includes the following columns:

- **Date**: The date of the recorded stock price.
- **Open**: The opening price of the stock on the given date.
- **High**: The highest price the stock reached during the trading day.
- **Low**: The lowest price the stock reached during the trading day.
- **Close**: The closing price of the stock on the given date.
- **Adj Close**: The adjusted closing price, which takes into account factors like dividends and stock splits.
- **Volume**: The trading volume (number of shares traded) for the stock on the given date.
- **Ignore**: A column for internal use or data handling, which can be disregarded for analysis.

## Query Example

To illustrate the usage of this dataset, consider the following query:

```sql
SELECT Date, Open, Close
FROM stock_data
WHERE Date BETWEEN '2021-01-01' AND '2021-06-30'
```

This query retrieves the 'Date', 'Open', and 'Close' columns from the 'stock_data' table for records falling within the first half of the year 2021.

# Method

This guide presents a comprehensive workflow for analyzing streaming data using AWS services, including AWS Glue, Lambda, and Apache Flink. The workflow revolves around the following components:

1. **Data Source**: Utilize the dataset named `AMDprices2021-2022.csv` as your streaming data source.

2. **Data Ingestion with Lambda**: Employ the script `lambda_function.py` to ingest data from the dataset into an Amazon Kinesis Data Stream.

3. **Analysis with Flink**: Utilize the Jupyter notebook `flink_notebook.ipynb` along with the accompanying notebook history file `notebook.zpln` for conducting real-time analysis with Apache Flink.

4. **Queries and Results**: Apply SQL queries using the files `query1CMGR.sql` for calculating the Compounded Monthly Growth Rate and `query2EMA.sql` for the Exponential Moving Average. The results, including visualizations, are presented in `query_result.pdf`.

## Prerequisites

Before embarking on the workflow, ensure you have the following prerequisites:

1. An active **AWS Account** with access to AWS services.
2. The dataset `AMDprices2021-2022.csv` containing your streaming data.
3. Familiarity with **AWS Lambda** for data ingestion.
4. Understanding of **AWS Glue** for ETL operations.
5. Basic knowledge of **Apache Flink** for real-time analysis.
6. Knowledge of Jupyter notebooks for interactive analysis.

## Workflow Steps

1. **Data Source**: Your streaming data originates from the `AMDprices2021-2022.csv` dataset.

2. **Lambda Data Ingestion**: Implement the `lambda_function.py` script to send data from the dataset to an Amazon Kinesis Data Stream for seamless ingestion.

3. **Interactive Analysis**: Engage with the Jupyter notebook `flink_notebook.ipynb` to interactively analyze the streaming data using Apache Flink. The notebook history is captured in `notebook.zpln`.

4. **SQL Queries and Results**: Apply SQL queries stored in `query1CMGR.sql` and `query2EMA.sql`. View the comprehensive query results, including visualizations, in the `query_result.pdf` document.

# Conclusion

This comprehensive workflow showcases the seamless integration of AWS Glue, Lambda, and Apache Flink to process, analyze, and visualize streaming data. With this approach, you can efficiently extract valuable insights from your data source, ultimately enabling informed decision-making.

For detailed code implementation, interactive notebook demonstrations, and further resources, refer to the documentation and files included in this repository. Feel free to customize the workflow to match your specific streaming data analysis requirements and objectives.
