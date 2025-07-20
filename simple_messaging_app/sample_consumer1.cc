#include <librdkafka/rdkafka.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>

const char *brokers = "<machine_ip>:9092";
const char *group_id = "group_1";
const char *auto_res = "earliest";


int main(int argc, char** argv)
{
    rd_kafka_t *consumer;
    rd_kafka_resp_err_t err;
    char errstr[512];
    rd_kafka_conf_t *conf;
    const char* topic1 = "topic1";
    conf = rd_kafka_conf_new();
     if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
     	std::cout << " broker set error: %s" << errstr << std::endl;
     	return 1;
    }
    if (rd_kafka_conf_set(conf, "group.id", group_id, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
     	std::cout << "group_id set error: %s" << errstr << std::endl;
     	return 1;
    }

    if (rd_kafka_conf_set(conf, "auto.offset.reset", auto_res, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
     	std::cout << "auto_res set error: %s" <<  errstr << std::endl;
     	return 1;
    }

    consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));

    if (!consumer) {
        std::cout << "Failed to create new consumer: %s" << errstr << std::endl;
    } else {
    	std::cout << "consumer is created" << std::endl;
    }

    rd_kafka_poll_set_consumer(consumer);

    // Configuration object is now owned, and freed, by the rd_kafka_t instance.
    conf = NULL;


    rd_kafka_topic_partition_list_t *subscription = rd_kafka_topic_partition_list_new(1);
    
    std::cout << "adding topic " << topic1 << std::endl;
    rd_kafka_topic_partition_list_add(subscription, topic1, RD_KAFKA_PARTITION_UA);
    
    // Subscribe to the list of topics.
    err = rd_kafka_subscribe(consumer, subscription);
    if (err) {
        std::cout << "Failed to subscribe to " << subscription->cnt
                  << " topics: " << subscription->cnt, rd_kafka_err2str(err);
        rd_kafka_topic_partition_list_destroy(subscription);
        rd_kafka_destroy(consumer);
    }

    rd_kafka_topic_partition_list_destroy(subscription);


    // Start polling for messages.
    while (1) {
        rd_kafka_message_t *consumer_message;

        consumer_message = rd_kafka_consumer_poll(consumer, 100);
        if (!consumer_message) {
            //std::cout << "Waiting..." << std::endl;
            continue;
        }

        if (consumer_message->err) {
            if (consumer_message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                /* We can ignore this error - it just means we've read
                 * everything and are waiting for more data.
                 */
                std::cout << "EOF Consumer error: "
                          << rd_kafka_message_errstr(consumer_message) << std::endl;
            } else {
                std::cout << "Consumer error: " << rd_kafka_message_errstr(consumer_message)
                          << std::endl;
            }
        } else {
            int key_len = (int)consumer_message->key_len;
            std::cout << "Consumed event from " << rd_kafka_topic_name(consumer_message->rkt)
                      << " key_len " << key_len << std::endl;
            if (key_len) {
                std::string key((char*)consumer_message->key, key_len); 
                std::cout << "key\n" << key << std::endl;
            }
            std::cout << "value\n" << (char*)consumer_message->payload << std::endl;
        }

        // Free the message when we're done.
        rd_kafka_message_destroy(consumer_message);
    }

    // Close the consumer: commit final offsets and leave the group.
    std::cout << " Closing consumer" << std::endl;
    rd_kafka_consumer_close(consumer);

    // Destroy the consumer.
    rd_kafka_destroy(consumer);

    return 0;
}
