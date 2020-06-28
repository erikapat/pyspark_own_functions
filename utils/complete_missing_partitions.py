import pyspark.sql.functions as psf
import pyspark.sql.functions as sf
from pyspark.sql import Window, DataFrame, column
from datetime import datetime, timedelta
from itertools import product
from pyspark.sql.types import DateType
from pyspark.sql.functions import unix_timestamp
from dateutil.relativedelta import relativedelta

def first_day_month(col_name: column) -> column:
    '''
    
    '''
    return unix_timestamp((sf.trunc(col_name, "month"))).cast("timestamp")

def list_intermediate_months(df: DataFrame, time_col: str) -> list:

    '''
    intermedite list of dates from a column
    '''
    #1.- min - max dates where we want to complete the data 
   #df = df.withColumn(time_col, first_day_month(time_col))
    min_max_timestamps = df.agg(sf.min(df[time_col]), sf.max(df[time_col])).head()
    
    # 2.-  create the intermediate dates
    first_date, last_date = [ts.date() for ts in min_max_timestamps]
    all_month_in_range = [first_date + relativedelta(months=d) for d in range((last_date.month - first_date.month) + 1)]
    
    return all_month_in_range


def complete_missing_months(df: DataFrame, time_col: str, referece_col: str, spark) -> DataFrame:
    
    '''
    Complete missing dates between rows
    df: dataframe with the missing rows
    time_col: columns of timestamps in format yyyy-MM-dd
    reference_col: column to maintein has reference (just one)
    '''
    
    df = df.withColumn(time_col, first_day_month(time_col))
    all_months_in_range =list_intermediate_months(df, time_col)

    reference_column = [row[referece_col] for row in df.select(referece_col).distinct().collect()]

    dates_by_var = spark.createDataFrame(product(reference_column, all_months_in_range),
                                                schema=(referece_col, time_col))

    #3. complete
    df2 = (df.join(dates_by_var,
                                    [time_col, referece_col],
                                    how="full")
               )
    return df2


