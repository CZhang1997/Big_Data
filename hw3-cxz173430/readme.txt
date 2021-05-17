@Author: Churong Zhang(cxz173430), Xizhen Yang(yxx180008)


spark version: 3.1.1
scala version: 2.12.13

initial setup:
make sure have kafka, elasticsearch, logstash, kibana downloaded
create kafka topic in the kakfa folder:
bin/kafka-topics.sh --create --topic topicA --bootstrap-server localhost:9092

moniter the topic:
bin/kafka-console-consumer.sh --topic topicA --from-beginning --bootstrap-server localhost:9092

turn on elasticsearch, kibana

 run logstash using the logstash.conf file

cd into the dir run sbt -> assembly
then run:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --class KafkaExample target/scala-2.12/kafka-assembly-0.1.jar topicA movies magic

then the data will start collecting and send to kafka elasticsearch in this formats
{
"tweets": Watch out for this creature crawling…,
"analyz": Neutral,
"value":2
}
tweets = the tweets
analyz = the sentiment evaluation of the Tweets
value  = sentiment evaluation value


part2 summary:
The dataset is contains information about a customer buying item A also buys item B. The results do make sense. They give the information about how each Amazon item relates to each other. I can get information about the importance of items based on the PageRank, and also give recommendation to customers. However, I do not see the difference of in-degree and out-degree relationship for our case.

how to run part 2:
use intellj build the project
then run this command
spark-submit --class WordCount target/scala-2.12/sparkwordcount_2.12-0.1.jar <input_file> <output_file>
<input_file> examples: amazon_data-full.txt or amazon_data-short.txt
<output_file> sample: out.txt
