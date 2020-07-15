import pyspark.sql.functions as psf
import pyspark.sql.functions as sf
from pyspark.sql import Window, DataFrame, column
from datetime import datetime, timedelta
from itertools import product
from pyspark.sql.types import DateType
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import col, count, isnan, lit, sum



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




def count_nulls_by_column(df, group_column: str = None):
    '''
    Count the number of missing values by group in several columns (only for numerical variables)
    '''
    def count_null(c):
        """Use conversion between boolean and integer
        - False -> 0
        - True ->  1
        """
        pred = col(c).isNull() | isnan(c)
        return sum(pred.cast("integer")).alias(c)
    
    #only for numerical variables
    exclude_cols = [c for c, t in df.dtypes if t in ('string', 'timestamp') ]
    df = df.drop(*exclude_cols)
    df_cols = df.columns
    if (group_column):
        df = df.groupby(group_column)

    return df.agg(*[count_null(c) for c in df_cols])

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


