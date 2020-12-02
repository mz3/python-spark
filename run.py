from pyspark.sql import SparkSession

testFile = "README.md"
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
testData = spark.read.text(testFile).cache()

numAs = testData.filter(testData.value.contains('a')).count()
numBs = testData.filter(testData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
