# Start SparkR
Sys.setenv(SPARK_HOME='/Users/liang/Downloads/spark-1.6.1-bin-hadoop2.6/')
.libPaths(c(file.path(Sys.getenv('SPARK_HOME'), 'R', 'lib'), .libPaths()))
library(SparkR)
library(MASS)
sc <- sparkR.init(master='local')
sqlContext <- sparkRSQL.init(sc)


#data Generation and Visualization
mu1 <- c(-4, -2)                         # Mean
mu2 <- c(4, -2)                         # Mean
mu3 <- c(0, 2)                         # Mean
Sigma <- matrix(c(1, 0, 0, 1), 2)  # Covariance matrix
cluster1 <- mvrnorm(100, mu = mu1, Sigma = Sigma )
cluster2 <- mvrnorm(100, mu = mu2, Sigma = Sigma )
cluster3 <- mvrnorm(100, mu = mu3, Sigma = Sigma )
data <- rbind(cluster1, cluster2, cluster3)
plot(data[,1],data[,2],xlab='x', ylab='y')

kPoints <- do.call(rbind, SparkR:::takeSample(df, FALSE, 10, 16189L))

# Training
df <- createDataFrame(sqlContext,data.frame(x = data[, 1],y = data[, 2]))
cache(df)
centroids <- mvrnorm(3, mu = c(0, 0), Sigma = matrix(c(2, 0, 0, 2), 2))
iterations <- 20
k <- 3



for (i in 1:iterations) {
  df$D1 <-(df$x-centroids[1,1])^2 + (df$y-centroids[1,2])^2
  df$D2 <-(df$x-centroids[2,1])^2 + (df$y-centroids[2,2])^2
  df$D3 <-(df$x-centroids[3,1])^2 + (df$y-centroids[3,2])^2
  
  #Find out the closest centroid if/else does not work here, so sign function is used for comparisons
  df$D1min <- (-sign(df$D1-df$D2)+1)/2 * (-sign(df$D1-df$D3)+1)/2
  df$D2min <- (-sign(df$D2-df$D3)+1)/2 * (-sign(df$D2-df$D1)+1)/2
  df$D3min <- (-sign(df$D3-df$D1)+1)/2 *(-sign(df$D3-df$D2)+1)/2 
  
  # not support vector/matrix serialization
  c1x <- collect(agg(df, sumg = sum(df$D1min * df$x)))[[1]]
  c1y <- collect(agg(df, sumg = sum(df$D1min  * df$y)))[[1]]
  c1Count <- collect(agg(df, sumg = sum(df$D1min)))[[1]]
  c2x <- collect(agg(df, sumg = sum(df$D2min * df$x)))[[1]]
  c2y <- collect(agg(df, sumg = sum(df$D2min * df$y)))[[1]]
  c2Count <- collect(agg(df, sumg = sum(df$D2min)))[[1]]
  c3x <- collect(agg(df, sumg = sum(df$D3min* df$x)))[[1]]
  c3y <- collect(agg(df, sumg = sum(df$D3min * df$y)))[[1]]
  c3Count <- collect(agg(df, sumg = sum(df$D3min)))[[1]]
  
  #Updates
  centroids[1,] <- c(c1x/c1Count,c1y/c1Count)
  centroids[2,] <- c(c2x/c2Count,c2y/c2Count)
  centroids[3,] <- c(c3x/c3Count,c3y/c3Count)
}

#Results
print(centroids)
points(centroids[,1],centroids[,2],col='red',pch=18,cex=1.5)