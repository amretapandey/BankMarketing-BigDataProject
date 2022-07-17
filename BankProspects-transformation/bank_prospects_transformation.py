# Create Spark Session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkDF').getOrCreate()



# Create a dataframe from the csv file
bankProspectsDF = spark.read.csv("bank_prospects.csv",header=True)


"""<br>

**1. Change the schema of the dataframe**
"""
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import col

bankProspectsDF1 = bankProspectsDF.withColumn("Age", col("Age").cast(IntegerType())).withColumn("Salary", col("Salary").cast(FloatType()))


"""**2. Remove tuples with unknown in Country attribute**

"""
bankProspectsDF2 = bankProspectsDF1.filter('Country != "unknown"')


"""**3. Replace missing Age values with average age**"""
from pyspark.sql.functions import mean

avgAge = bankProspectsDF2.select(mean("Age")).collect()[0][0]
bankProspectsDF3 = bankProspectsDF2.na.fill(avgAge, "Age")


"""**4. Replace missing Salary values with average salary**"""
avgSalary = bankProspectsDF3.select(mean("Salary")).collect()[0][0]
bankProspectsDF4 = bankProspectsDF3.na.fill(avgSalary, "Salary")


"""# Saving the transformed dataframe to csv"""
bankProspectsDF4.write.format("csv").save("bank_prospects-transformed")

