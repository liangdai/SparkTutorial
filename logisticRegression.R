# Start SparkR
Sys.setenv(SPARK_HOME='/Users/liang/Downloads/spark-1.6.1-bin-hadoop2.6/')
.libPaths(c(file.path(Sys.getenv('SPARK_HOME'), 'R', 'lib'), .libPaths()))
library(SparkR)
sc <- sparkR.init(master='local')
sqlContext <- sparkRSQL.init(sc)

#data Generation and Visualization
dataGeneration <- function(w, size){
  x1 = runif(size, -10, 10)
  x2 = runif(size, -10, 10)
  noise = rnorm(size, 0, 2)
  v = x1 * w[1] + x2 * w[2] + w[3] + noise
  y = (v>0) * 2-1
  return(data.frame(x1,x2,y))
}

w <- c(8, -3, -1)
size <- 100
data <- dataGeneration(w, size)
colorlist <- c("blue","red")
plot(data$x1,data$x2,xlab='x1', ylab='x2',col=colorlist[(data$y+3)/2])
x1 <- c(-10, 10)
x2 <- -(x1 * w[1] + w[3]) / w[2]
lines(x1, x2, col='black')


# Training
df <- createDataFrame(sqlContext, data)
cache(df)
set.seed(10)
w <- rnorm(3, 0, 1)
cat("Initial w: ", w, "\n")
learningRate = 0.05
n <- count(df)
iterations <- 10
savedW <- matrix(NA, iterations+1, 3)
savedW[1, ] <- w
for (i in 1:iterations) {
  df$tmp <- exp(-df$y*(df$x1*w[1]+df$x2*w[2]+w[3])) + 1
  df$gradientw1 <- df$tmp/(df$tmp+1)*df$y*df$x1
  df$gradientw2 <- df$tmp/(df$tmp+1)*df$y*df$x2
  df$gradientw3 <- df$tmp/(df$tmp+1)*df$y
  w[1] <- w[1] - learningRate * collect(agg(df, sumg = sum(df$gradientw1)))[[1]]/n
  w[2] <- w[2] - learningRate * collect(agg(df, sumg2 = sum(df$gradientw2)))[[1]]/n
  w[3] <- w[3] - learningRate * collect(agg(df, sumg2 = sum(df$gradientw3)))[[1]]/n
  savedW[i+1, ] <- w
}


# Result
x1 <- c(-10, 10)
x2 <- -(x1 * w[1] + w[3]) / w[2]
plot(x1, x2, col='red',type='l',lwd=5, main="w")
x2 <- -(x1 * savedW[8,1] + savedW[8,3]) / savedW[8,2]
lines(x1, x2,lty=2,col="deepskyblue",lwd=3)
x2 <- -(x1 * savedW[9,1] + savedW[9,3]) / savedW[9,2]
lines(x1, x2,lty=3,col="green",lwd=3)
x2 <- -(x1 * savedW[10,1] + savedW[10,3]) / savedW[10,2]
lines(x1, x2, col='blue',lty=4,lwd=3)
x2 <- -(x1 * savedW[11,1] + savedW[11,3]) / savedW[11,2]
lines(x1, x2, col='black',lty=4,lwd=3)
legend("topleft",c("True w","Iteration 7","Iteration 8", "Iteration 9", "Iteration 10"),lty=1:5,col=c("red","deepskyblue","green",'blue','black'),lwd=3,cex = 0.75)

