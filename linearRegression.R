# Start SparkR
Sys.setenv(SPARK_HOME='/Users/liang/Downloads/spark-1.6.1-bin-hadoop2.6/')
.libPaths(c(file.path(Sys.getenv('SPARK_HOME'), 'R', 'lib'), .libPaths()))
library(SparkR)
sc <- sparkR.init(master='local')
sqlContext <- sparkRSQL.init(sc)

#data Generation and Visualization
dataGeneration <- function(w, size){
  x = runif(size, -10, 10)
  noise = rnorm(size, 0, 2)
  y = x * w[1] + w[2] + noise
  return(data.frame(x,y))
}

w <- c(4, 2)
size <- 100
data <- dataGeneration(w, size)
plot(data$x,data$y,xlab='x', ylab='y')
x <- c(-10, 10)
y <- x * w[1] + w[2]
lines(x, y, col='red')


# Training
df <- createDataFrame(sqlContext, data)
cache(df)
w <- c(0, 0)
cat("Initial w: ", w, "\n")
learningRate = 0.01
n <- count(df)
iterations <- 100
savedW <- matrix(NA, iterations+1, 2)
savedW[1, ] <- w
for (i in 1:iterations) {
  df$gradientx <- (df$y - df$x * w[1] - w[2]) * df$x * (-2)
  df$gradientbias <- (df$y - df$x * w[1] - w[2]) * (-2)
  w[1] <- w[1] - learningRate * collect(agg(df, sumg = sum(df$gradientx)))[[1]]/n
  w[2] <- w[2] - learningRate * collect(agg(df, sumg2 = sum(df$gradientbias)))[[1]]/n
  savedW[i+1, ] <- w
}

# Result
x <- c(-10, 10)
y <- x * w[1] + w[2]
plot(x, y, col='red',type='l',lwd=5, main="w")
y <- x * savedW[1,1] + savedW[1,2]
lines(x, y,lty=2,col="deepskyblue",lwd=3)
y <- x * savedW[2,1] + savedW[2,2]
lines(x, y,lty=3,col="green",lwd=3)
y <- x * savedW[3,1] + savedW[3,2]
lines(x, y, col='blue',lty=4,lwd=3)
y <- x * savedW[4,1] + savedW[4,2]
lines(x, y, col='indianred',lty=5,lwd=3)
legend("topleft",c("True w","Iteration 0","Iteration 1", "Iteration 2", "Iteration 3"),lty=1:5,col=c("red","deepskyblue","green",'blue','indianred'))

