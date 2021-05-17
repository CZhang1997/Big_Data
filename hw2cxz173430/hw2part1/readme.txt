@Author: Churong Zhang (cxz173430), Xizhen Yang(xxy180008)

Part 1:
files:
PageRank source code
pagerank_2.11-0.1.jar - jar file that can run in aws
hw2part1data.csv - input data
part-00000  - example output using 100 iterations

Usage:
1. upload pagerank_2.11-0.1.jar and hw2part1data.csv to aws s3 bucket,
2. open a cluster in EMR with spark installed
3. go to steps -> add step, pick spark app, enter
--class "PageRank" as option
pick pagerank_2.11-0.1.jar as the app
enter the 3 argu
input file: s3/../hw2part1data.csv
iterations: 100
output dir: s3/../../
