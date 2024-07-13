from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
with open('/media/cattoocule/e7/study/Data-Engineer/Projects/WordCount_Pyspark_Airflow/text.txt', 'r') as f:
    words = spark.sparkContext.parallelize(f.read().split(" "))
    wordcounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)

for wc in wordcounts.collect():
    print(wc[0], wc[1])


spark.stop()