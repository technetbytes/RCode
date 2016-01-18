#Created on Thu Jan 1 20:09:54 2016
#author: saqibullah
#email: saqibullah@gmail.com

# Include Header.R file in WC_MRJob.R file 
source(file="Header.R")

# Define wordcount mapper function  
wc.map.fn <- function(k,lines) {   
  words.list <- strsplit(lines, '\\s')   
  words <- unlist(words.list)   
  return( keyval(words, 1) )   
} 

# Define wordcount reduce function 
wc.reduce.fn <- function(word, counts) {   
  keyval(word, sum(counts))   
}

 
# Define mapreduce function set reference in map and reduce  
wordcount <- function (input, output=NULL) {   
  mapreduce(input=input, output=output, input.format="text", map=wc.map.fn, reduce=wc.reduce.fn)   
} 

 
# Set root path
hdfs.root <- '/user/saqib/wordcount/' 

# Remove out folder if exist 
hdfs.rmr('/user/saqib/wordcount/out') 

# Set hdfs.data property as a file source 'files' is a folder inside wordcount 
hdfs.data <- file.path(hdfs.root, 'files') 

# Set hdfs.out property as a output folder 
hdfs.out <- file.path(hdfs.root, 'out') 

# Call wordcount mapreduce function 
out <- wordcount(hdfs.data, hdfs.out) 
 
# Get results from HDFS 
results <- from.dfs(out) 
results.df <- as.data.frame(results, stringsAsFactors=F) 
colnames(results.df) <- c('word', 'count') 
head(results.df)