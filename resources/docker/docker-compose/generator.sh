#!/bin/bash
function create_kafka_topic {
    $KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic $1
}
function send_messages_to_kafka {
    msg=$(generator_message)
    echo -e $msg | $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic $TOPIC
}
function rand {
    min=$1
    max=$(($2-$min+1))
    num=$(date +%s%N)
    echo $(($num%$max+$min))
}
function generator_message {
    uid=$(rand 1 999);
    product_id=$(rand 1 999);
    timestamp=`date '+%s'`;
    action=$(rand 1 3);
    msg=$uid","$product_id","$timestamp","$action;
    echo $msg
}

#TOPIC="log"
TOPIC="con"
create_kafka_topic $TOPIC
while true
do
 send_messages_to_kafka
 sleep 0.1
done
