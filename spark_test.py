from pyspark.sql import SparkSession
from pyspark import SparkConf

spark_conf = SparkConf()
spark_conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
spark_conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark_conf.set("spark.hadoop.fs.s3a.access.key", "AKIA4M5NUBNCSSFF3RLN")
spark_conf.set("spark.hadoop.fs.s3a.secret.key", "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")
# spark_conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
# spark_conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
# spark_conf.set("spark.driver.bindAddress", "127.0.0.1")

spark = SparkSession.builder \
    .appName("test_data_input") \
    .config(conf=spark_conf) \
    .getOrCreate()

existing_parquet_path = "s3://test-ml-storage/de-test/aizen_de_test_1.parquet"
existing_df = spark.read.parquet(existing_parquet_path)
