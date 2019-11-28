#Homework 3


##Monte Carlo Simulations for stocks





Steps to run the application on Cloudera Quickstart VM: 

* sbt clean assembly
* copy the jar to the VM 

Deploying the command on the VM : 

run command :


`spark-submit --class driver_monteCarlo --master local  --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 --queue default chinmay_nautiyal_hw3-assembly-0.1.jar <input-path-here>`

The input data is made available through a shell script used as documented here: 
`./get-yahoo-quotes.sh <Stock Symbol>`
###Overview of the Spark Application: 

This application tries to simulate the day trader's situation, who has stock history available to her, and tries to make the decision of investing in a stock by randomly sampling a distribution and randomly transforming the historical data across many parallel simulations. 


The application makes use of the parallel processing capabilities made available by Apache Spark. The stock data is imported into a DataFrame which is queried and used to perform various calculations. 

The link to the youtube video, documenting the deployment process on AWS EMR and Google DataProc is provided below: 

[Link to Youtube Video]( https://youtu.be/oXGkJk2cXp4)
