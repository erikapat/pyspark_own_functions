
# Operations with partitions

import pyspark.sql.functions as sf
from pyspark.sql import Window, DataFrame, column
from datetime import datetime
from dateutil.relativedelta import relativedelta

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
    return datetime.strftime(datetime.strptime(date_value, '%Y%m%d') - relativedelta(days=1),
                              '%Y%m%d'
                             )

def sustract_partition(date_value: str, data_depth_months: int) -> str:
    return datetime.strftime(datetime.strptime(date_value, '%Y%m%d') -
                                   relativedelta(months=data_depth_months),
                                   '%Y%m%d'
                                  )     

### does not work
def init_partition_df(date_value: column, data_depth_months: int) -> column:
    '''
    Similar to init_date but for dataframes
    Sustract and show de the first day of the month
    '''
    
    return datetime.strftime(datetime.strptime(date_value, 'yyyyMMdd') -
                              relativedelta(months=data_depth_months)+
                              relativedelta(days=1),
                              '%Y%m%d'
                             )

# PENDING
'''
reference_date = '20191231' # This is the date for the computation of the attribute
data_depth_months = 12      # This is the depth of the input data that will be included

# Init date for monthly computations:
date_init = datetime.strftime(datetime.strptime(reference_date, '%Y%m%d') -
                              relativedelta(months=data_depth_months)+
                              relativedelta(days=1),
                              '%Y%m%d'
                             )


PART_FIELD = 'partition_id'


from datetime import datetime

# Init for the partitions that should be taken into account
partition_init           = datetime.strftime(datetime.strptime(reference_date, '%Y%m%d') -
                                   relativedelta(months=data_depth_months),
                                   '%Y%m%d'
                                  )                          

_FORMAT_PART_FIELD_MOV_  = 'yyyyMMdd'
part_field_name          = 'partition_id'

#for movements
WIND_DAYS_MOV = 5

# Init for the movements' partitions that should be taken into account
mov_part_init_date = datetime.strftime(datetime.strptime(partition_init, '%Y%m%d') -
                                       relativedelta(days = WIND_DAYS_MOV),
                                       '%Y%m%d'
                                      )

mov_part_end_date = datetime.strftime(datetime.strptime(reference_date, '%Y%m%d') +
                                       relativedelta(days = WIND_DAYS_MOV),
                                       '%Y%m%d'
                                      )


all_et_al =  df_movs
all_et_al = (all_et_al
                        .withColumn('partition_id',
                                    sf.date_format(
                                        sf.date_sub(sf.add_months(sf.trunc(sf.col('fec_movim_real'),'month'),
                                                                  1),
                                                    1),
                                        "yyyyMMdd")
                                   )
                       )

'''