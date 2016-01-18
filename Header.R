#Created on Thu Jan 1 19:29:54 2016
#author: saqibullah
#email: saqibullah@gmail.com

# RHadoop Header  
  
# Step 1 - Set the environment variables  
Sys.setenv(HADOOP_HOME="/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/")  
Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")   
Sys.setenv(HADOOP_STREAMING="/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming-2.6.0-cdh5.5.1.jar")   
 
# Step 2 - Load the required libraries  
library(rmr2)  
library(rJava)  
library(rhdfs)  
 
# Step 3 - Initialize HDFS  
hdfs.init()