package com.gargle.common.kafka.producer;

import com.gargle.common.config.GargleConfig;
import com.gargle.common.exception.GargleException;
import com.gargle.common.utils.string.StringUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Properties;

/**
 * ClassName:BaseProducer
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/11/30 09:48
 */
public abstract class BaseProducer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(BaseProducer.class);

    protected static String log_prefix = "[kafka-producer-client]-";

    private volatile boolean start = false;

    private GargleConfig gargleConfig;

    private KafkaProducer<K, V> producer;

    @PostConstruct
    public synchronized void init() {
        if (start) {
            return;
        }
        start = true;
        gargleConfig = getGargleConfig();
        GargleConfig.KafkaProducerConfig producerConfig = getProducerConfig();
        if (!producerConfig.isEnable()) {
            logger.warn("{}未开启.", log_prefix);
            return;
        }

        Properties properties = producerConfig.buildProperties();
        producer = new KafkaProducer<>(properties);
        log_prefix = "[kafka-producer-client-" + getProducerName() + "]-";
        logger.info("{}生产者客户端已准备完毕.", log_prefix);
    }

    @PreDestroy
    public void close() {
        producer.close();
        logger.info("{}生产者已关闭,资源已释放.", log_prefix);
    }

    public void produce(String topic, V value) {
        produce(topic, null, value);
    }

    public void produce(String topic, K key, V value) {
        produce(topic, null, key, value);
    }

    public void produce(String topic, Integer partition, K key, V value) {
        produce(topic, partition, null, key, value);
    }

    public void produce(String topic, Integer partition, Long timestamp, K key, V value) {
        producer.send(new ProducerRecord<K, V>(topic, partition, timestamp, key, value, null));
    }

    protected GargleConfig.KafkaProducerConfig getProducerConfig() {
        if (gargleConfig == null) {
            throw new NullPointerException(log_prefix + "初始化 gargleConfig 为空!");
        }

        String producerName = getProducerName();
        if (StringUtil.isBlank(producerName)) {
            throw new GargleException("BaseProducer-初始化存在producerName为空的子类!");
        }

        List<GargleConfig.KafkaProducerConfig> kafkaProducers = gargleConfig.getKafkaProducers();
        if (kafkaProducers == null || kafkaProducers.size() == 0) {
            throw new GargleException("BaseProducer-初始化kafkaProducers配置不存在 producerName为: "
                    + producerName + " 的kafka消费者配置. kafkaProducers is null");
        }

        for (GargleConfig.KafkaProducerConfig kafkaProducer : kafkaProducers) {
            if (kafkaProducer == null) {
                continue;
            }

            if (producerName.equals(kafkaProducer.getProducerName())) {
                return kafkaProducer;
            }
        }

        throw new GargleException("BaseProducer-初始化kafkaProducers配置不存在 producerName为: "
                + producerName + " 的kafka消费者配置.");
    }

    public abstract GargleConfig getGargleConfig();

    public abstract String getProducerName();
}