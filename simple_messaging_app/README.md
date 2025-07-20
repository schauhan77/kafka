This is a simple messaging app to demostrate kafka message transfer between producer and consumer

Things needed for running the app:

i) Need to create kafka cluster
We can download binary from https://kafka.apache.org/downloads and follow below steps 

after downloading on a machine :
we can create cluster using kraft
update the config for config/kraft/broker.properties and config/kraft/controller.properies to sample config given under configs/ in this folder.
You need to change ips to your machine ip or localhost

How to run cluster:
a) Generate an ID for the Kafka cluster
   > bin/kafka-storage.sh random-uuid
b) Set up log directories for each node [broker or controller] using same uuid
   > bin/kafka-storage.sh format -t XZ-zdeleT9GUd3IHr54lsg -c config/kraft/controller.properties
   > bin/kafka-storage.sh format -t XZ-zdeleT9GUd3IHr54lsg -c config/kraft/broker.properties
c) Run controller and broker
   > bin/kafka-server-start.sh config/kraft/broker.properties
   > bin/kafka-server-start.sh config/kraft/controller.properties

ii) Install librdkafka library
https://github.com/confluentinc/librdkafka


Imp points:
i) you need to mention broker ip:port [<machine_ip>:9092] in sample_consumer1.cc and sample_publisher1.cc


Compile sample_consumer1.cc and sample_publisher1.cc like this :
g++ sample_consumer1.cc -I /libkafka/common/common/librdkafka/include/ /lib64/librdkafka.so.1
g++ sample_producer1.cc -I /libkafka/common/common/librdkafka/include/ /lib64/librdkafka.so.1

Above path should be updated accordingly


Running app:
i) start the cluster by running controller and broker as mentioned above
ii) Run consumer [binary of above compilation] and then run producer to send your msg

sample consumer and producer output on running:

consumer:
> ./a.out
consumer is created
adding topic topic1
EOF Consumer error: Broker: No more messages
Consumed event from topic1 key_len 10
key
sample_key
value
Hello
this is my first event-msg
EOF Consumer error: Broker: No more messages

Producer:
> ./a.out
published to topic: topic1
after flush
message(s) were delivered




