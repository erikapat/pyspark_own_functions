from pyspark.sql.functions import month
#from pyspark.sql.functions import row_number
#from pyspark.sql.functions import to_date
#from pyspark.sql.functions import last_day
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col
from pyspark.sql.functions import explode, array, struct
from typing import List


def melt_function(df: DataFrame, by: str)-> DataFrame:
    from pyspark.sql.functions import col, explode, array, struct
    '''
    Function similar to melt. Transform columns in rows. This is useful
    for aggregations and graphs
    '''
    # Filter dtypes and split into column names and type description
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
    # only homogeneous columns
    assert len(set(dtypes)) == 1, "All columns have to be of the same type"

    # Create and explode an array of (column_name, column_value) structs
    kvs = explode(array([
      struct(lit(c).alias("field"), col(c).alias("movs")) for c in cols
    ])).alias("kvs")

    return df.select(by + [kvs]).select(by + ["kvs.field", "kvs.movs"])

def arrays_last_n_df_rows(df_att: DataFrame,
                         number_months: int,
                         field_type: str,
                         metric: str,
                         custom_id : str,
                         date_init: str,
                         partition_id: str) -> DataFrame:
    '''
    Create a column of type array of the last n rows of a df

    '''

    # 1. Select columns of interest
    df = (df_att
          .filter( col(partition_id) > str(date_init) )
          .select(custom_id, 'month', field_type, metric)
         )
   # 2. Transform from rows to columns (pivot)
    reshaped_df = df.groupby(custom_id, field_type).pivot('month').max(metric).fillna(0)

    # 3. Reverse pivot - melt - from columns to rows
    df_melt = to_explode(reshaped_df, [custom_id, field_type])#.withColumnRenamed("field", "month")


    #---- This is important to guarantee order (improve)
    #6. Count number of months in each category (this to guarantee n (= 'number_months') months)
    #df_row = (df_melt.select(custom_id, 'field', field_type, 'movs')
    #           .withColumn("row_num", row_number().over(Window.partitionBy(custom_id, field_type)
    #           .orderBy(col(custom_id).desc(),col(field_type).desc(), col('field').desc()))))
    # 7. Filter and sort to have and ordered list
    #df_row = df_row.filter(col('row_num') <= number_months).sort(custom_id, field_type, 'field')
    #___
   
    # 8. Collect in an array
    df_f = (df_melt.groupBy(custom_id, field_type)
       .agg(sf.collect_list('movs').alias('values')))
   
    return df_f


'''
#susbstract values from a column


Subtract functions
-----------------------------------------------------------------------------------------------------
from pyspark.sql.functions import length
def contract_values(df, contract_number, contract_name):
    df = (df
    .withColumn('leng', length(col(contract_number)))
    .withColumn('leng9', length(col(contract_number)) - 9 )
    .withColumn('leng19', length(col(contract_number)) - (9 + 26) )
    .withColumn(contract_name, sf.col(contract_number)
                .substr(length(col(contract_number)) - (9 + 26),  length(col(contract_number)) - 18  )) #14
    .withColumn('leng_final', length(col(contract_name)))

          )
    return df

'''


def to_explode(df: DataFrame, by: List[str]) -> DataFrame:
    """
    Function similar to melt. Transform columns in rows. This is useful
    for aggregations and graphs

    :param df: Original dataframe.
    :param by:
    :return DataFrame
    """

    # Filter dtypes and split into column names and type description
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
    # only homogeneous columns
    assert len(set(dtypes)) == 1, "All columns have to be of the same type"

    # create and explode an array of (column_name, column_value) structs
    kvs = explode(array([
    struct(lit(c).alias("field"), col(c).alias("movs")) for c in cols
    ])).alias("kvs")

    return df.select(by + [kvs]).select(by + ["kvs.field", "kvs.movs"])
