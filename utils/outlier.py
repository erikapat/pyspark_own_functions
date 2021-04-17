from pyspark.sql.functions import mean as mean, stddev 
from pyspark.sql.functions import col , column
import pyspark.sql.functions as sf

import numpy as np
import pyspark.sql.functions as func
from pyspark.sql.types import FloatType

def median(values_list):
    med = np.median(values_list)
    return float(med)
udf_median = func.udf(median, FloatType())

def detectOutlier(values: column, UpperLimit: column, LowerLimit: column) -> column:
    '''
    outliers are points lying below LowerLimit or above upperLimit
    (values < LowerLimit) or (values > UpperLimit)
    '''
    return (values < LowerLimit) | (values > UpperLimit)

def limits(df, comparison_column, field_value):
    '''
    Calculate median, mean, variance of by group and set the limits for outliers
    '''

    df = (df.groupby(comparison_column).agg(mean(field_value).alias("mean"), stddev(field_value).alias("stddev"), sf.skewness(field_value).alias("skewness"),
          (udf_median(func.collect_list(col(field_value))).alias('median')))
    .withColumn("UpperLimit", col("mean") + col("stddev")*3)
    .withColumn("LowerLimit", col("mean") - col("stddev")*3))
    return df
    
def find_outliers(df, comparison_column, field_value, keep_columns = []):
    '''
    Return thos values that are outliers
    df: dataframe with the values
    comparison_column: regarding to whom you are an outlier
    filed_value: field with the possible outliers
    keep_columns: columns to keep from the original data.frame in []
    '''
    
    statsDF = limits(df, comparison_column, field_value)

    joinDF = df.select([comparison_column, field_value] + keep_columns).join(statsDF, comparison_column, 'left')
    
    outlierDF = joinDF.withColumn("isOutlier", detectOutlier(col(field_value), col("UpperLimit"), col("LowerLimit"))).filter(col("isOutlier"))
    
    outlierDF = other_statistical_ratios(outlierDF)
    
    print(outlierDF.drop(comparison_column, 'monthly_partition', 'isOutlier').columns)
    for c in outlierDF.drop(comparison_column, 'monthly_partition', 'isOutlier').columns:
        outlierDF = outlierDF.withColumn(c, sf.round(c, 3))
    
    return outlierDF

def other_statistical_ratios(df):
    '''

    '''
    
    df = df.withColumn('ratio_mean_var', col('mean')/col('stddev'))
    df = df.withColumn('ratio_mean_var', (col('stddev')*col('stddev'))/col('mean'))
    df = df.withColumn('Galton', (col('mean') - col('median'))/col('stddev'))
    
    return(df)