# Data Processing in Apache Kafka

# Start services

sudo systemctl start zookeeper
sudo systemctl start kafka

# Task-1 Create a topic named atscale with 2 parts and a replication factor of 1 on Kafka

kafka-topics.sh --bootstrap-server localhost:9092 --create --topic atscale --partitions 2 --replication-factor 1

# Task-2 List the existing topics on Kafka

kafka-topics.sh --bootstrap-server localhost:9092 --list

# Task-3 Print the atscale topic properties on the screen

kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic atscale

# delete topic

kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic atscale