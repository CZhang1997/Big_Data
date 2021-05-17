@Author: Churong Zhang (cxz173430), Xizhen Yang(xxy180008)

Part 2:
files:
Tweet source code
tweet_2.11-0.1.jar - jar file that can run in aws
Tweets.csv - input data
part-00000  - example output (label, prediction)

Usage:
1. upload tweet_2.11-0.1.jar and Tweets.csv to aws s3 bucket,
2. open a cluster in EMR with spark installed
3. go to steps -> add step, pick spark app, enter
--class "Tweet" as option
pick tweet_2.11-0.1.jar as the app
enter the 3 argu
input file: s3/../Tweets.csv
output dir: s3/../../
