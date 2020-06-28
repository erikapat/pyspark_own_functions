
# Operations with partitions

import pyspark.sql.functions as sf
from pyspark.sql import Window, DataFrame, column
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import unix_timestamp

def montly_partition_YYmmdd(col_date: column) -> column:
    '''
    Time to 'yyyy-MM-dd 00:00:00'
    
    col_date: dataframe with the name of the column of interest
    '''

    return unix_timestamp(col_date, 'yyyyMMdd').cast("timestamp")



def create_partitions_from_df(col_name: column) -> column:
    '''
    Each date with their month partition format yyyyMMdd
    '''
    return sf.date_format(sf.date_sub(sf.add_months(sf.trunc(col_name, 'month'),1),1), "yyyyMMdd")

def create_partitions_from_df_sep(col_name: column) -> column:
    '''
    Each date with their month partition format yyyy-MM-dd
    '''
    return sf.date_format(sf.date_sub(sf.add_months(sf.trunc(col_name, 'month'),1),1), "yyyy-MM-dd")


def init_day_partition(date_value: str) -> str:
    '''
    Show the first day of the month of the partition
    '''
    return datetime.strftime(datetime.strptime(date_value, '%Y%m%d') - relativedelta(months= 1) +
                             relativedelta(days=1),
                              '%Y%m%d'
                             )

def sustract_month_partition(date_value: str, data_depth_months: int) -> str:
    return datetime.strftime(datetime.strptime(date_value, '%Y%m%d') -
                                   relativedelta(months=data_depth_months),
                                   '%Y%m%d'
                                  )     

def sustract_days_partition(date_value: column, data_depth_days: int) -> str:
    return datetime.strftime(datetime.strptime(date_value, '%Y%m%d') -
                                           relativedelta(days = data_depth_days),
                                           '%Y%m%d'
                                          )
def add_days_partition(date_value: column, data_depth_days: int) -> str:
    return datetime.strftime(datetime.strptime(date_value, '%Y%m%d') +
                                           relativedelta(days = data_depth_days),
                                           '%Y%m%d'
                                          )
