from pyspark.sql.functions import mean as mean, stddev 
from pyspark.sql.functions import col , column
import pyspark.sql.functions as sf
from pyspark.sql.functions import sqrt
from pyspark.sql import DataFrame
from typing import List

import numpy as np
import pyspark.sql.functions as func
from pyspark.sql.types import FloatType

def median(values_list):
    med = np.median(values_list)
    return float(med)
udf_median = func.udf(median, FloatType())

#create function to calculate Mahalanobis distance
def mahalanobis(x=None, data=None, cov=None):

    x_mu = x - np.mean(data)
    if not cov:
        cov = np.cov(data.values.T)
    inv_covmat = np.linalg.inv(cov)
    left = np.dot(x_mu, inv_covmat)
    mahal = np.dot(left, x_mu.T)
    return mahal.diagonal()
udf_mahalanobis = func.udf(mahalanobis, FloatType())

def mySd(col: column) -> column:
    return sqrt(avg(col * col) - avg(col) * avg(col))


def detectOutlier(values: column, UpperLimit: column, LowerLimit: column) -> column:
    '''
    outliers are points lying below LowerLimit or above upperLimit
    (values < LowerLimit) or (values > UpperLimit)
    values: column to compare
    UpperLimit: upper threshold that differentiate normal from anomalous
    LowerLimit: lower threshold 
    '''
    return (values < LowerLimit) | (values > UpperLimit)

def limits(df: DataFrame, comparison_column: str, field_value: str, lower_sigma: int = 3, upper_sigma: int = 3):
    '''
    Calculate median, mean, variance of by group and set the limits for outliers
    '''
    
    df = df.withColumn('x2', col(field_value)*col(field_value) )
    df = (df.groupby(comparison_column).agg(mean(field_value).alias("mean"), 
                                            (udf_median(func.collect_list(col(field_value))).alias('median')),
                                            sf.skewness(field_value).alias("skewness"),
                                            stddev(field_value).alias("stddev"), 
                                            ((udf_median(func.collect_list(col('x2'))) - udf_median(func.collect_list(col(field_value)))*udf_median(func.collect_list(col(field_value))))).alias("stddev_median") # sqrt
                                           )
    .withColumn("UpperLimit", col("mean") + col("stddev")*upper_sigma)
    .withColumn("LowerLimit", col("mean") - col("stddev")*lower_sigma))
    return df
    
def find_outliers(df: DataFrame, comparison_column: str, field_value: str, keep_columns: List[str] = [], 
                  only_outliers: bool  = True, lower_sigma: int = 3, upper_sigma: int = 3):
    '''
    Return those values that are outliers
    df: dataframe with the values
    comparison_column: regarding to whom you are an outlier
    filed_value: field with the possible outliers
    keep_columns: columns to keep from the original data.frame in []
    '''
    
    statsDF = limits(df, comparison_column, field_value, lower_sigma, upper_sigma)

    joinDF = df.select([comparison_column, field_value] + keep_columns).join(statsDF, comparison_column, 'left')
    
    outlierDF = joinDF.withColumn("isOutlier", detectOutlier(col(field_value), col("UpperLimit"), col("LowerLimit")))
    
    print('number of outliers: ', outlierDF.filter(col("isOutlier")).count())
    print('number of Normal values: ', outlierDF.filter(~col("isOutlier")).count())
    if (only_outliers == True):
        outlierDF = outlierDF.filter(col("isOutlier"))
    
    outlierDF = other_statistical_ratios(outlierDF)
    
    for c in outlierDF.drop(comparison_column, 'monthly_partition', 'isOutlier').columns:
        outlierDF = outlierDF.withColumn(c, sf.round(c, 3))
    
    return outlierDF

def other_statistical_ratios(df: DataFrame):
    '''

    '''
    
    df = df.withColumn('ratio_mean_sd', col('mean')/col('stddev'))
    df = df.withColumn('ratio_sd_mean', (col('stddev'))/col('mean'))
    df = df.withColumn('Hotelling', (col('mean') - col('median'))/col('stddev'))
    
    return(df)