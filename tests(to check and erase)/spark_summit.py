
from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
    example program to show how to submit applications
    """

spark = SparkSession\
.builder\
.appName("LowerSongTitles")\
.getOrCreate()

log_of_songs = [
"Despacito",
"All other Stars"
"Despacito",
"despacito",
"All other Stars"
]

distributed_song_long = spark.sparkContext.parallelize(log_of_songs)
print(distributed_song_long.map(lambda x: x.lower().collect()))

      
spark.stop()