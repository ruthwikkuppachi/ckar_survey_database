# CKAR Survey Database ETL Script

An ETL script written in Python that moves data from Google Sheets to MongoDB with the help of pymongo.

## About
This repository contains an ETL script, that was developed as part of our semester-long project with CKAR CDC (https://ckarcdc.org/), a community development non-profit based in Riverdale Park, Maryland. The main goal of this project was to move all of their survey data from Google Sheets, where the data was collected due to the surveys being administered through Google Forms, to MongoDB Atlas, a cloud-based NoSQL database. The survey contained numerous questions that aimed to understand the problems faced by the Latino Community in Greater Riverdale area.

## Description
The following technologies were used to complete this project:
- Python (pandas, pymongo)
- MongoDB Atlas (including MongoDB Charts)
- AWS Lambda
- AWS S3
- Google Sheets API

The first task within this project was to extract the survey responses from Google Sheets and store the responses temporarily as a dataframe in a Python script. A Google Developer account was created to create the credentials to access the spreadsheet programatically. After the responses were stored in a pandas dataframe, I did some preprocessing with the data like standardizing the data values, adding new fields as metadata, and using regular expressions to get the data in a desired format. This Python script was hosted on AWS Lambda which allowed us to run the script without worrying about any servers. 

After the data was processed, the next step was to dump the data into a MongoDB Atlas collection. First of all, the script had to open a connection with the required MongoDB collection. The database credentials were stored as environment variables on AWS Lambda.  Next, before inserting the data, some checks were need to be made to ensure there is no duplicate data in the database. If a given survey response passed the checks, it was converted into a dictionary and inserted one-by-one into the colelction. As of now, there are around 1000 survey responses stored in the database. 

The final step was to create a dashboard based on the survey data that will help our non-technical clients analyze the data and get an overview of answers to important questions in the survey quickly and without writing any code. To create the dashboard, we made use of the MongoDB Charts feature that comes with MongoDB Atlas. The dashboard displays key information like demographic information about survey respondents, some helpful programs, and some of the challenges that the members of the Latino community are facing in the Greater Riverdale Area. The link to the dashboard is: https://charts.mongodb.com/charts-ckar-survey-database-gnfid/public/dashboards/60906fee-7c34-4884-87f3-416a904c8c3f.

The following diagram provides an overview of the entire pipeline.

![image](https://user-images.githubusercontent.com/51654907/135184154-7cd67d38-0f17-4071-b910-bb16e8cc377c.png)

It was important for us to automate this entire process of data extraction to dashboard creation since our clients didn't have the technical expertise to perform these tasks like running the scripts manually. Therefore, we made use of AWS EventBridge to automatically run the Python script every day, to fetch new data and store this data into the database. The charts are also automatically updated everyday. 
