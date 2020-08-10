#from pyspark.sql import DataFrame
#from pyspark.sql.functions import col,  isnan
import pyspark.sql.functions as sf
from pyspark.sql import Window, DataFrame, column
from datetime import datetime, timedelta
from itertools import product
from pyspark.sql.types import DateType
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import col, count, isnan, lit, sum
from dateutil.relativedelta import relativedelta
import sys



# check duplicates

def check_duplicates(df: DataFrame, listOfColumns: str = None)-> DataFrame:
    if df.count() > df.dropDuplicates(listOfColumns).count():
        print('Data has duplicates')

    return df \
        .groupby(listOfColumns) \
        .count() \
        .where('count > 1') \
        .sort('count', ascending=False) \
        .show()




def check_duplicates2(df: DataFrame, listOfColumns: str = None)-> DataFrame:
    if df.count() > df.dropDuplicates(listOfColumns).count():
        raise ValueError('Data has duplicates')

    

#----------------------------------------- --------------------------------------------------------------------------------------------
#count nulls

def count_nulls_by_column(df: DataFrame, group_column: str = None)-> DataFrame:
    '''
    Count the number of missing values by group in several columns (only for numerical variables)
    '''
    def count_null(c):
        """
        Use conversion between boolean and integer
        - False -> 0
        - True ->  1
        """
        pred = col(c).isNull() | isnan(c)
        return sum(pred.cast("integer")).alias(c)
    
    #only for numerical and string variables
    exclude_cols = [c for c, t in df.dtypes if t in ( 'timestamp') ]
    df = df.drop(*exclude_cols)
    df_cols = df.columns
    if (group_column):
        df = df.groupby(group_column)

    return df.agg(*[count_null(c) for c in df_cols])