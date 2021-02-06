# Zomato-API-Data-Analysis

Objective:
Big Data is shorthand for advancing trends in technology that open the door to a new approach to understanding the world and making decisions.
A lot of organizations are making use of these huge amounts of data to make their customer experience by personalizing marketing, increasing automation and better forecasting.
Especially the business of delivering restaurant meals to the home is undergoing rapid change as new online platforms race to capture markets and customers across the world.
So for our project, we wanted to explore data from a restaurant aggregator and food delivery application called ‘Zomato’ through different data exploration approaches.

Project Outline:
- This project's objective is to extract live data from the API and experiment with it by stream it into both SQL and NoSQL Databases and analyze them in.
- In the first approach, we streamed the data from Zomato API into MySQL and saved it in required formats.
- Then from MySQL we transferred the data. Using Sqoop we exported that data into HDFS and performed analysis using HiveQL. 
- For Data visualization, we used ‘Tableau’ software.
- For the second approach we loaded the data into Mongo db. Obtained the data from this db using pyspark script and saved it in HDFS. From HDFS saved it into Hive table and did analysis using pyspark in Zeppelin.

Data Description:
- Extracted data from the Zomato developers API by generating an API key and by using python. 
- The data was collected using the Zomato API and has information about the Restaurant name, Location, Cuisine, Ratings and other demographics wherein each restaurant is    identified uniquely by an ID.
- The columns are of data types like INT and VARCHAR/String.
- Few uniquely identified columns are online delivery and takeaway, whose values  are numerical, one meaning ‘yes’ and the other meaning ‘no’
- The data was saved in 2 formats – JSON and CSV

