from pyspark.sql.functions import mean as mean, stddev 
from pyspark.sql.functions import col , column

def detectOutlier(values: column, UpperLimit: column, LowerLimit: column) -> column:
    '''
    outliers are points lying below LowerLimit or above upperLimit
    (values < LowerLimit) or (values > UpperLimit)
    '''
    return (values < LowerLimit) | (values > UpperLimit)

def find_outliers(df, comparison_column, filed_value):
    '''
    Return thos values that are outliers
    df: dataframe with the values
    comparison_column: regarding to whom you are an outlier
    filed_value: field with the possible outliers
    '''
    
    statsDF = (df.groupby(comparison_column).agg(mean(filed_value).alias("mean"), stddev(filed_value).alias("stddev"))
    .withColumn("UpperLimit", col("mean") + col("stddev")*3)
    .withColumn("LowerLimit", col("mean") - col("stddev")*3))

    joinDF = df.select('account_id', 'year', filed_value, 'monthly_partition').join(statsDF, comparison_column, 'left')

    outlierDF = joinDF.withColumn("isOutlier", detectOutlier(col(filed_value), col("UpperLimit"), col("LowerLimit"))).filter(col("isOutlier"))
    
    return outlierDF