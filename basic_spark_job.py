from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("BasicCSVQuery").getOrCreate()

# Read sample CSV from S3
input_path = "s3://keerthan-buckets3/Day 9/sample-data.csv"

df = spark.read.option("header", "true").csv(input_path)

# Register as SQL view
df.createOrReplaceTempView("people")

# Run a SQL query (e.g., select all people over 30)
result_df = spark.sql("""
    SELECT name, age
    FROM people
    WHERE age > 30
""")

# Write result to S3 as Parquet
output_path = "s3://keerthan-buckets3/Day 9/"
result_df.write.mode("overwrite").parquet(output_path)

print("✅ Query completed and results written to S3.")
