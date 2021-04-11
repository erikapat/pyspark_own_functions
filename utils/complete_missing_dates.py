import pyspark.sql.functions as psf
import pyspark.sql.functions as sf
from pyspark.sql import Window, DataFrame, column
from datetime import datetime, timedelta
from itertools import product
from pyspark.sql.types import DateType
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import col, count, isnan, lit, sum
from dateutil.relativedelta import relativedelta
import sys
from utils.partitions import *


##########
# days   #
##########

def daily_date_YYmmdd(col_date: column) -> column:
    '''
    Time to 'yyyy-MM-dd 00:00:00'
    
    col_date: dataframe with the name of the column of interest
    '''

    return unix_timestamp(sf.date_format(col_date, "yyyy-MM-dd"), 'yyyy-MM-dd').cast("timestamp")

def list_intermediate_dates(df: DataFrame, time_col: str) -> list:

    '''
    intermedite list of dates from a column
    '''
    #1.- min - max dates where we want to complete the data 
    min_max_timestamps = df.agg(psf.min(df[time_col]), psf.max(df[time_col])).head()
    
    # 2.-  create the intermediate dates
    first_date, last_date = [ts.date() for ts in min_max_timestamps]
    all_days_in_range = [first_date + timedelta(days=d)
                         for d in range((last_date - first_date).days + 1)]
    
    return all_days_in_range
    
def complete_missing_days(df: DataFrame, time_col: str, referece_col: str, spark) -> DataFrame:
    
    '''
    Complete missing dates between rows
    df: dataframe with the missing rows
    time_col: columns of timestamps in format yyyy-MM-dd
    reference_col: column to maintein has reference (just one)
    '''
    
    #------------------------------------------------------
    #Further improvement
    #Do a function that verificate if the columns exists
    #------------------------------------------------------
    
    # 1.- list of intermediates dates
    all_days_in_range = list_intermediate_dates(df, time_col)
    # 2.- create the complete dataset
    reference_column = [row[referece_col] for row in df.select(referece_col).distinct().collect()]
    
    dates_by_var = spark.createDataFrame(product(reference_column, all_days_in_range),
                                            schema=(referece_col, time_col))
    

    #3. complete
    df2 = (df.join(dates_by_var,
                                [time_col, referece_col],
                                how="full")
           )

    return df2
#------------------------------------------------------------------------------------------------------------------------------------------------
##########
# months #
##########

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
    delta_months = (last_date.year - first_date.year) * 12 + last_date.month - first_date.month
    all_month_in_range = [first_date + relativedelta(months=d) for d in range((delta_months) + 1)] 
    #all_month_in_range = [first_date + relativedelta(months=d) for d in range((last_date.month - first_date.month) + 1)] 
    
    return all_month_in_range


def complete_missing_months(df: DataFrame, time_col: str, referece_col: str, spark) -> DataFrame:
    
    '''
    Complete missing dates between rows
    df: dataframe with the missing rows
    time_col: columns of timestamps in format yyyy-MM-dd
    reference_col: column to maintein has reference (just one)
    '''
    
    df = df.withColumn('monthly_partition', create_partitions_from_df(time_col, "yyyy-MM-dd").cast("timestamp"))
    #.withColumn('date_aux', first_day_month(time_col))
    all_months_in_range =list_intermediate_months(df, time_col)
    #print(all_months_in_range)

    reference_column = [row[referece_col] for row in df.select(referece_col).distinct().collect()]

    dates_by_var = spark.createDataFrame(product(reference_column, all_months_in_range),
                                                schema=(referece_col, time_col))

    #3. complete
    df2 = (df.join(dates_by_var,
                                    [time_col, referece_col],
                                    how= "full")
               )
    return df2 #.drop('date_aux')



#--------------------------------------------------------------------------------------------------

def complete_with_forward_value(df, group_column, to_complete_col, date_column):
    wind = (Window
                .partitionBy(group_column)
                .rangeBetween(Window.unboundedPreceding, -1)
                .orderBy(psf.unix_timestamp(date_column))
                )
    df = df.withColumn('completed_column',
                             psf.last(to_complete_col, ignorenulls = True).over(wind))
    df = df.select(
            group_column,
            psf.coalesce(date_column, psf.to_timestamp(date_column)).alias("timestamp"),
            psf.coalesce(to_complete_col, 'completed_column').alias(to_complete_col))
    
    return df

def complete_with_backward_value(df, group_column, to_complete_col, date_column):
    
    wind = (Window.partitionBy(group_column)
               .orderBy(date_column)
               .rowsBetween(0,sys.maxsize))
    
    df = df.withColumn('completed_column',
                             psf.first(to_complete_col, ignorenulls = True).over(wind) )
    df = df.select(
            group_column,
            psf.coalesce(date_column, psf.to_timestamp(date_column)).alias("timestamp"),
            psf.coalesce(to_complete_col, 'completed_column').alias(to_complete_col))
    
    return df

#--------------------------------------------------------------------------------------------------

def df_col_rename(X, to_rename, replace_with):
    """
    :param X: spark dataframe
    :param to_rename: list of original names
    :param replace_with: list of new names
    :return: dataframe with updated names
    """
    import pyspark.sql.functions as F
    mapping = dict(zip(to_rename, replace_with))
    X = X.select([F.col(c).alias(mapping.get(c, c)) for c in to_rename])
    return X


