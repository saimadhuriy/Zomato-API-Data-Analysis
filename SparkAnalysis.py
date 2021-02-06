# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 22:24:58 2020

@author: yerra
"""

pyspark --jars "/root/mongo-hadoop-spark-2.0.2.jar" --driver-class-path "/root/mongo-java-driver-3.4.3.jar"  --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.2

# Create saprk session object to retrieve data from mongo db
spark = SparkSession.builder.appName("Restaurant_Analysis").config("spark.mongodb.input.uri","mongodb://127.0.0.1/rest_db.rest_collection").config("spark.mongodb.output.uri","mongodb://127.0.0.1/rest_db.rest_collection").getOrCreate()
# Save data in mongo db as a spark data frame
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# Check the schema of the dataframe
df.printSchema()
# Check first five rows of the data
df.show(5)
# Check the summarys statistics of the numerical data
df.describe.show()
# Check the columns of the data
df.columns
 
# Select required columns from the dataframe and drop duplicates and save it in a new dataframe
rest_df = df.select('restaurant_id', 'name', 'country_id', 'city', 'locality', 'cuisines', 'avg_cost_for_two', 'price_range', 'currency', 'has_online_delivery', 'include_bogo_offers', 'has_table_booking', 'aggregate_rating', 'rating_text', 'votes', 'all_reviews_count').dropDuplicates()

# Check the schema, first five columns, summary stats and column names
rest_df.printSchema()
rest_df.show(5)
rest_df.describe.show()
rest_df.columns

# Write the data into hdfs in csv format
df.write.format("csv").save("hdfs:///user/maria_dev/tmp/myData.csv")



%spark2.pyspark

# Import required packages
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt                                                                                                                    
import seaborn as sns 
import numpy as np
import pandas as pd
from pyspark.sql.functions import avg
from pyspark.sql.functions import max
import pyspark.sql.functions
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType

# Create spark session object to get data frim hive tables
spark = SparkSession.builder.appName("res_app").master("local").enableHiveSupport().getOrCreate()

# Save the data into a spark data frame
df = spark.sql("select * from restaurant_db.restaurants")

# Check the schema of the data
df.printSchema()
# Check first five rows of the dataframe
df.show(5)
# Check the column names
df.columns
# Check the summary statistics of the numerical columns
df.describe().show()

# Change the data type of the 'has_online_delivery' and 'has_table_booking' columns from 'int' to 'string'
df = df.withColumn("has_online_delivery", df["has_online_delivery"].cast(StringType()))
df = df.withColumn("has_table_booking", df["has_table_booking"].cast(StringType()))

# Check the schema to verify the type is converted
df.printSchema()

# create SQL context
sqlContext = SQLContext(sc)

# Convert the spark dataframe to spark dataframe
df_pandas = df.toPandas()

# Replace all the '1's with 'True' and all '0's to 'False' in the columns 'has_online_delivery' and 'has_table_booking'
df_pandas['has_online_delivery'] = df_pandas['has_online_delivery'].apply(lambda x: x.replace("1", "True"))
df_pandas['has_online_delivery'] = df_pandas['has_online_delivery'].apply(lambda x: x.replace("0", "False"))
df_pandas['has_table_booking'] = df_pandas['has_table_booking'].apply(lambda x: x.replace("1", "True"))
df_pandas['has_table_booking'] = df_pandas['has_table_booking'].apply(lambda x: x.replace("0", "False"))

# Check the uniques values of 'has_online_delivery' and 'has_table_booking' columns, if the values are changed
df_pandas.has_online_delivery.unique()
df_pandas.has_table_booking.unique()

# plot the distplot to check the distribution of 'avg_cost_for_two' 
sns.distplot(df_pandas['avg_cost_for_two'])
plt.title("Distribution of average cost for two")
plt.show()

#plot the distplot to check the distribution of 'aggregate_rating'
sns.distplot(df_pandas['aggregate_rating'])
plt.title("Distribution of aggregate rating of restuarants")
plt.show()

# display maximum and minimum values of avg_cost_for_two and aggregate_rating columns
df.select([max("aggregate_rating"), max("avg_cost_for_two")]).show()
df.select([min("aggregate_rating"), min("avg_cost_for_two")]).show()

# Set the style of the graph to white and plot a lineplot with labelled axes and title for Average cost for two people vs Aggregate Rating for restuarants with/wthout online delivery
sns.set(style="white")
graph = sns.lineplot(x="aggregate_rating", y="avg_cost_for_two",
                  hue="has_online_delivery", data=df_pandas)
plt.xlabel("Aggregate Rating")
plt.ylabel("Average cost for two people")
plt.title("Average cost for two people vs Aggregate Rating with/wthout online delivery")
plt.show()

# plot the heat map to the correlations between various attributes considered
sns.heatmap(df_pandas.corr(), annot=True, linewidths=0.1, cmap='RdYlGn')
plt.show()

# Check the number of restaurants for each city
df.groupby('city').count().show()
# display the uniques cities present in data 
df.select('city').distinct().show()

# Create a new data frame with few selected cities and display first 10 rows of the data
df_cities = df.select('city', 'avg_cost_for_two', 'rating_text', 'all_reviews_count').filter((f.col('city') == 'Los Angeles') | (f.col('city') == 'Chennai') | (f.col('city') == 'Bangalore') | (f.col('city') == 'Lucknow') | (f.col('city') == 'Kochi'))
df_cities.show(10)

# Convert newly created spark dataframe into pandas dataframe
df_cities_pd = df_cities.toPandas()

# Display a barplot for Average cost for two people vs City for different rating texts
ax = sns.barplot(x="city", y="avg_cost_for_two", hue="rating_text", data=df_cities_pd)
plt.title("Average cost for two people vs City for different rating texts")
plt.show()

# Create a temporary view to excute sql queries
df.createOrReplaceTempView("res_table")

# Display top 10 localities with high average cost for two people
query = '''SELECT locality, avg_cost_for_two FROM res_table ORDER BY avg_cost_for_two DESC'''
sqlContext.sql(query).show(10)

# Get mean average cost for two for each city
df.select('city', 'avg_cost_for_two').groupby('city').avg('avg_cost_for_two').show()

# Get mean average cost for two for each rating text written by customers
df_cities.groupby('rating_text').avg('avg_cost_for_two').show()

# Set the style of the graph to white and plot a lmplot with labelled axes and title for Average cost for two people vs Votes for different price ranges
sns.set(style="white")
g1 = sns.lmplot(y="avg_cost_for_two", x="votes", hue="price_range",
                     height=5, data=df_pandas)
g1.set_axis_labels("Votes", "Average cost for two people")
plt.title("Average cost for two people vs Votes for different price ranges")
plt.show()

# Get top 10 comibinations of votes and their corresponding average cost for two in descending order
df.select('avg_cost_for_two', 'votes').orderBy(["avg_cost_for_two", "votes"], ascending=[0, 0]).show(10)

# Get distinct values of 'include_bogo_offers' column
df.select('include_bogo_offers').distinct().show()

# Convert the data type of include_bogo_offers column into string type and verify that
df = df.withColumn("include_bogo_offers", df["include_bogo_offers"].cast(StringType()))
df.printSchema()

# Get the count of restaurants in selected cities with bogo offers
df.filter(df.include_bogo_offers == 'true').filter((f.col('city') == 'Los Angeles') | (f.col('city') == 'Chennai') | (f.col('city') == 'Bangalore') | (f.col('city') == 'Lucknow') | (f.col('city') == 'Kochi')).groupby('city').count().orderBy('city').show()

# Display a relplot between Average cost for two people and all review count for different price ranges
sns.relplot(x="all_reviews_count", y="avg_cost_for_two", hue="price_range",
            sizes=(40, 400), alpha=.5, palette="muted",
            height=6, data=df_pandas)
plt.title("Average cost for two people vs all review count for different price ranges")
plt.show()

# Get top 5 restuarants including their names, avg cost for two people, total review count, price range with highest total review counts 
df.select('name', 'avg_cost_for_two', "all_reviews_count", 'price_range').orderBy(["all_reviews_count"], ascending=[0]).show(5)

# Get distinct values of cuisines
df.select('cuisines').distinct().show()

# Get 10 most frequent cuisines which restuarants has
query = '''SELECT cuisines, COUNT(name) AS count FROM res_table GROUP BY cuisines ORDER BY count DESC'''
sqlContext.sql(query).show(10)

# select restaurant information with selected cuisines and save it in a dataframe and check first four rows of data
df_cuisine = df.select('name', 'cuisines', 'avg_cost_for_two', 'aggregate_rating', 'has_online_delivery').filter((f.col('cuisines') == 'Pizza, Fast Food') | (f.col('cuisines') == 'North Indian') | (f.col('cuisines') == 'Fast Food, Burger') | (f.col('cuisines') == 'Burger, Fast Food') | (f.col('cuisines') == 'American') | (f.col('cuisines') == 'Italian'))
df_cuisine.show(4)

# convert the newly formed dataframe into pandas dataframe
df_pd2 = df_cuisine.toPandas()

# Plot a lmplot with labelled axes and title for Aggregate rating of restaurant vs type of cuisine
sns.boxplot(x="aggregate_rating", y="cuisines", data=df_pd2,
            whis="range", palette="vlag")
plt.xlabel("Aggregate Rating")
plt.ylabel("Type of Cuisine")
plt.title("Aggregate rating of restaurant vs type of cuisine")
plt.show()

#  Get the average aggregate rating for various cuisines and display top 10 in descending order
query = '''SELECT cuisines, AVG(aggregate_rating) AS avg FROM res_table GROUP BY cuisines ORDER BY avg DESC'''
sqlContext.sql(query).show(10)

#  Get the mean average cost for 2 people for various cuisines and display top 10 in descending order
query = '''SELECT cuisines, AVG(avg_cost_for_two) AS avg FROM res_table GROUP BY cuisines ORDER BY avg DESC'''
sqlContext.sql(query).show(10)

# Set the style of the graph to white and plot a barplot with labelled axes and title for Average cost for two people vs type of cuisine with/without online delivery
plt.style.use('ggplot')
plt.figure(figsize=(15,5))
g2 = sns.barplot(x="cuisines", y="avg_cost_for_two", hue="has_online_delivery", data=df_pd2)
plt.xlabel("Type of cuisine")
plt.ylabel("Average cost for two people")
plt.title("Average cost for two people vs type of cuisine with/without online delivery")
plt.show()

# Get the maximum value of aggregate rating for each cuisine
df_cuisine.groupby('cuisines').max("aggregate_rating").show()
# Get the maximum value of average cost for two people for each cuisine`   
df_cuisine.groupby('cuisines').max("avg_cost_for_two").show()