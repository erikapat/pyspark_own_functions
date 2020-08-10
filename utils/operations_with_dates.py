from pyspark.sql import DataFrame
from utils.Spark import get_spark
import pandas as pd
from pyspark.sql.types import DateType


def get_range_dates_df(init_date: str, end_date: str) -> DataFrame:
    """
    Creates a DataFrame of dates as rows between given params
    :param init_date:
    :param end_date:
    :return:
    """
    spark = get_spark()
    range_list = pd.date_range(init_date, end_date).to_pydatetime().tolist()
    dates = spark.createDataFrame(range_list, DateType()).withColumnRenamed("value", "gf_value_date")
    return dates


''' 
#(check more)

from datetime import datetime
import pyspark.sql.functions as sf
from dateutil.relativedelta import relativedelta
from pyspark.sql import Column
from pyspark.sql.functions import col


def filter_between_dates_with_margin(reference_date: str,
                                     date_column: str,
                                     months_depth: int,
                                     days_margin: int = 5,
                                     ) -> Column:

    """
     Filters date column between reference_date and 'depth' months before, adding a margin
     of 5 days(default) above and below.
    :param reference_date:
    :param date_column:
    :param months_depth:
    :param days_margin:
    :return:
    """
    init_partition = datetime.strftime(datetime.strptime(reference_date, '%Y%m%d') -
                                       relativedelta(months=months_depth) -
                                       relativedelta(days=days_margin),
                                       '%Y%m%d'
                                       )

    reference_plus_margin = datetime.strftime(datetime.strptime(reference_date, '%Y%m%d') +
                                              relativedelta(days=days_margin),
                                              '%Y%m%d'
                                              )

    return col(date_column).between(init_partition, reference_plus_margin)


def diff_months(init: Column, end: Column) -> Column:
    """
    Computes diff in (int) months between column end and column init
    """
    return null_or_greatest(sf.lit(1), sf.ceil(sf.months_between(end, init)))


def diff_years(init: Column, end: Column) -> Column:
    """
    Computes diff in (int) years between column end and column init
    """
    return null_or_greatest(sf.lit(1), sf.ceil(sf.months_between(end, init) / sf.lit(12)))


def null_or_least(c1: Column, c2: Column) -> Column:
    """
    Returns the lower value of both columns or null in case one of both contains a null value
    """
    return (sf.when(c1.isNull(), c1)
            .otherwise(sf.when(c2.isNull(), c2)
                       .otherwise(sf.least(c1, c2))
                       )
            )


def null_or_greatest(c1: Column, c2: Column) -> Column:
    """
    Returns the highest value of both columns or null in case one of both contains a null value
    """
    return (sf.when(c1.isNull(), c1)
            .otherwise(sf.when(c2.isNull(), c2)
                       .otherwise(sf.greatest(c1, c2))
                       )
            )


def get_month(col_name: str) -> Column:
    """
    This function transforms a date Column into a date Column that contains the
    year and month of the original date.

    For instance, 2019/12/24 will be transformed into 2019/12/01

    :param col_name: Original Column name.
    :return Column with the transformed date
    """
    return sf.trunc(sf.col(col_name), "month")


def get_n_month_windows(col_name: str,
                        ref_date: str,
                        n_months: int,
                        format_date: str = "yyyyMMdd") -> Column:
    """
    This function returns the ordinal of the n-month window in which the input date Column
    is included w.r.t. a given reference date.

    For instance, if n equals 12, the input Column contains 2019/12/24 and 2018/12/24,
    and the reference date is 2020/03/24, the output will be transformed into 0 and 1,
    respectively.

    :param n_months:
    :param col_name: Original Column name.
    :param ref_date: string that represents a date.
    :param format_date: string that represents the format of ref_date. Default is 'yyyyMMdd'
    :return Column with ordinal of the 12-month window in which the input Column is included.
    """

    return sf.floor(sf.months_between(sf.to_date(sf.lit(ref_date), format_date),
                                      sf.col(col_name)) / sf.lit(n_months))
                                      
                                      
                                      
    from typing import List

from dateutil.relativedelta import relativedelta
from pipeforge.utils.DatesUtils import add_months, PARTITION_FORMAT, monthly_partitions, partition_to_datetime, \
    datetime_to_partition


def get_n_monthly_partitions_back(partition: str, n_months: int) -> List[str]:
    """
    Gets n monthly partitions back since a given partition. For example, for partition = "20200228" and n_months = 1
    the returned partition is one, just the given partition "20200228". If partition = "20200228" and n_months = 2 the
    returned partitions are "20200131" and "20200228" as they equal to two moths since the given partition.
    """
    return monthly_partitions([add_months(partition, -n, PARTITION_FORMAT) for n in range(n_months)])


def beginning_of_monthly_partition(monthly_partition: str):
    """
    Get the first day of a monthly partition
    :param monthly_partition:
    :return:
    """
    return datetime_to_partition(partition_to_datetime(monthly_partition).replace(day=1))


def daily_partitions_between(start_partition: str, end_partition: str):
    """
    Get list of partitions between two partitions (included)
    :param start_partition:
    :param end_partition:
    :param step_days:
    :return:
    """
    start_date = partition_to_datetime(start_partition)
    end_date = partition_to_datetime(end_partition)
    delta_days = (end_date - start_date).days

    return [datetime_to_partition(start_date + relativedelta(days=i)) for i in range(delta_days + 1)]
                                      
                                      
    '''