#start  Apache zookeeper:
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties


# start kafka server :
.\bin\windows\kafka-server-start.bat .\config\server.properties

# create a kafka topic :
.\bin\windows\kafka-topics.bat --create --topic firstTopic --bootstrap-server localhost:9092


# start the producer :
.\bin\windows\kafka-console-producer.bat --topic firstTopic --bootstrap-server localhost:9092


# start the consumer :
.\bin\windows\kafka-console-consumer.bat --topic firstTopic --from-beginning --bootstrap-server localhost:9092


