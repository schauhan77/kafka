#include "publisher.hh"
#include <cstring>

Publisher* Publisher::publisher_ = NULL;

Publisher*
Publisher::instance(std::string brokers) {
    if (!publisher_) {
        publisher_ = new Publisher(brokers);
    }
    return publisher_;
}

Publisher::Publisher(std::string brokers):
    brokers_(brokers),
    producer_handle_(NULL),
    topic_conf_(NULL)
{
    init();
}


bool
Publisher::init()
{
    char errstr[512];

    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers_.c_str(),
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        std::cout << "Error: " << errstr;
        init_status_ = BROKERS_INIT_FAIL;
        return false;
    }

    topic_conf_ = rd_kafka_topic_conf_new();
    if (rd_kafka_topic_conf_set(topic_conf_, "acks", "all",
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        std::cout << "Error: " << errstr;
        init_status_ = BROKERS_INIT_FAIL;
        return false;
    }

    /* Create Kafka producer handle */
    if (!(producer_handle_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
                                          errstr, sizeof(errstr)))) {
        std::cout << "Error: " << errstr;
        init_status_ = PRODUCER_HANDLE_INIT_FAIL;
        return false;
    }
    return true;
}


bool
Publisher::publish_msg(const char * topic, void* payload, size_t payload_len,
                       void* key, size_t key_len)
{
    rd_kafka_resp_err_t err;

    char errstr[512];
    topic_conf_ = rd_kafka_topic_conf_new();
    if (rd_kafka_topic_conf_set(topic_conf_, "acks", "all",
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        std::cout << "Error: " << errstr;
        init_status_ = BROKERS_INIT_FAIL;
        return false;
    }

    rd_kafka_topic_t *rkt = rd_kafka_topic_new(producer_handle_, topic,
                                               topic_conf_);
    int er = rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                              payload, payload_len, key, key_len, NULL);

    if(er == -1) {
        err = rd_kafka_last_error();
        std::cout << "Failed to produce to topic: " << topic << " : " <<
            rd_kafka_err2str(err) << std::endl;
        return false;
    } else {
        std::cout << "published to topic: " << topic<<std::endl;
    }
    rd_kafka_flush(producer_handle_, 10*1000);
    std::cout << "after flush" <<std::endl;

    if (rd_kafka_outq_len(producer_handle_) > 0) {
        std::cout <<"message(s) were not delivered: " <<
            rd_kafka_outq_len(producer_handle_) << std::endl;
        return false;
    } else {
        std::cout <<"message(s) were delivered" << std::endl ;
    }
    return true;

}

InitStatus
Publisher::init_status()
{
    return init_status_;
}


int main()
{
    std::string top = "topic1";
    std::string payl = "Hello\nthis is my first event-msg";
    std::string ke = "sample_key";
    char* topic = (char*)top.c_str();
    char* payload = (char*)payl.c_str();
    char* key = (char*)ke.c_str();
    size_t key_len = strlen(key);
    size_t payload_len = strlen(payload);
    std::string brokers = "<machine_ip>:9092";
    Publisher::instance(brokers)->publish_msg(topic, payload, payload_len, key, key_len);
    return 0;
}
