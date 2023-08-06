%flink.pyflink

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf
from datetime import datetime

# Unregister the existing function if it exists
st_env.execute_sql("DROP TEMPORARY FUNCTION IF EXISTS calculate_cmgr")
    
# Define the custom function
@udf(result_type=DataTypes.FLOAT(), input_types=[DataTypes.STRING(), DataTypes.FLOAT()])
def calculate_cmgr(date_string: str, close_price: float) -> float:
    start_date = datetime.strptime("01/04/2021", "%m/%d/%Y").date()
    current_date = datetime.strptime(date_string, '%m/%d/%Y').date()
    
    # Check if the day is within the first three days of the month and not January 2021
    if current_date.day <= 3 and not (current_date.month == 1 and current_date.year == 2021):
        # Compute the number of months between the start date and the current date
        number_of_months = (current_date.year - start_date.year) * 12 + (current_date.month - start_date.month)
        
        # Calculate the CMGR
        cmgr = ((close_price / 92.3) ** (1 / number_of_months) - 1) * 100
        return cmgr
    else:
        return 0.0

# Register the custom function with a name
st_env.create_temporary_function("calculate_cmgr", calculate_cmgr)


++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

%flink.ssql(type=update)

SELECT event_time, calculate_cmgr(`date`, close_price) AS cmgr, `date`, close_price
FROM stock_table
WHERE calculate_cmgr(`date`, close_price) <> 0.0
