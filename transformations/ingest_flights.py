from pyspark.sql import SparkSession
from pyspark import pipelines as dp

# Get or create SparkSession for standalone Spark programs
spark = SparkSession.builder.appName("Avionics-Ingest").getOrCreate()

# Import and register the OpenSky datasource 
from pyspark_datasources import OpenSkyDataSource
spark.dataSource.register(OpenSkyDataSource)

# Declare a streaming table
@dp.table
def ingest_flights():
    # Read from the OpenSky source into a stream
    return spark.readStream.format("opensky").load()