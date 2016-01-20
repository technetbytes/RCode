#Created on Thu Jan 20 10:09:44 2016
#author: saqibullah
#email: saqibullah@gmail.com

# utility function - insert new row into exist data frame
insertRow <- function(target.dataframe, new.day) {
  new.row <- c(new.day, 0)
  target.dataframe <- rbind(target.dataframe,new.row)
  target.dataframe <- target.dataframe[order(c(1:(nrow(target.dataframe)-1),new.day-0.5)),]
  row.names(target.dataframe) <- 1:nrow(target.dataframe)
  return(target.dataframe)
}

mapper = function(null, line) {
  # skip header
  if( "ts" != line[[2]] )
    keyval(line[[1]], paste(line[[1]],line[[2]], line[[3]], sep=","))
}

reducer = function(key, val.list) {
  # not possible to build good enought regression for small datasets
  if( length(val.list) < 10 ) return;

  list <- list()
  # extract country
  country <- unlist(strsplit(val.list[[1]], ","))[[1]]
  # extract time interval and click number 
  for(line in val.list) {
    l <- unlist(strsplit(line, split=","))
    x <- list(as.POSIXlt(as.Date(l[[2]]))$mday, l[[3]])
    list[[length(list)+1]] <- x
  }
  # convert to numeric values
  list <- lapply(list, as.numeric)
  # create frames
  frame <- do.call(rbind, list)
  colnames(frame) <- c("day","clicksCount")

  # set 0 count of clicks for missed days in input dataset
  i = 1
  # we must have 15 days in dataset
  while(i < 16) {
    if(i <= nrow(frame))
      curDay <- frame[i, "day"]

    # next Day in existing frame is not suspected
    if( curDay != i ) {
      frame <- insertRow(frame, i)
    } 
    i <- i+1
  }

  # build lineral model for prediction
  model <- lm(clicksCount ~ day, data=as.data.frame(frame))
  # predict for the next day
  p <- predict(model, data.frame(day=16))

  keyval(country, p)
}

# Remove out folder if exist 
hdfs.rmr('/user/saqib/out')

# call MapReduce job
mapreduce(input="/user/saqib/in",
      input.format=make.input.format("csv", sep = ","),
      output="/user/saqib/out",
      output.format="csv",
      map=mapper,
      reduce=reducer
)