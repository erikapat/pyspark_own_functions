from pyspark.sql.functions import mean as mean, stddev 
from pyspark.sql.functions import col , column

def detectOutlier(values: column, UpperLimit: column, LowerLimit: column) -> column:
    '''
    outliers are points lying below LowerLimit or above upperLimit
    (values < LowerLimit) or (values > UpperLimit)
    '''
    return (values < LowerLimit) | (values > UpperLimit)

def find_outliers(df, comparison_column):
    '''
    Return thos values that are outliers
    df: dataframe with the values
    comparison_column: regarding to whom you are an outlier
    '''
    
    statsDF = (df.groupby(comparison_column).agg(mean("sum_amount").alias("mean"), stddev("sum_amount").alias("stddev"))
    .withColumn("UpperLimit", col("mean") + col("stddev")*3)
    .withColumn("LowerLimit", col("mean") - col("stddev")*3))

    joinDF = df.select('account_id', 'year', 'sum_amount', 'monthly_partition').join(statsDF, ['year'], 'left')

    outlierDF = joinDF.withColumn("isOutlier", detectOutlier(col("sum_amount"), col("UpperLimit"), col("LowerLimit"))).filter(col("isOutlier"))
    
    return outlierDF