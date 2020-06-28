rom pyspark.sql.functions import month
#from pyspark.sql.functions import row_number
#from pyspark.sql.functions import to_date
#from pyspark.sql.functions import last_day
import pyspark.sql.functions as F
from datetime import datetime


def to_explode(df: DataFrame, by: str)-> DataFrame:
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

def arrays_last_n_months_old(df_att: DataFrame,
                         number_months: int,
                         field_type: str,
                         metric: str,
                         custom_id : str,
                         date_init: str,
                         partition_id: str) -> DataFrame:
    '''
    Create a column with a list of values considering antiquity (NaN) and months without movements (0)

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