package com.gargle.common.kafka.consumer;

import com.gargle.common.config.GargleConfig;
import com.gargle.common.exception.GargleException;
import com.gargle.common.utils.string.StringUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ClassName:BaseConsumer
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/11/29 18:40
 */
public abstract class BaseConsumer<K, V, R> implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(BaseConsumer.class);

    protected static String log_prefix = "[kafka-consumer-client]-";

    private static final AtomicLong count = new AtomicLong(0);

    private static final AtomicLong THREAD_SUFFIX = new AtomicLong(0);

    private volatile boolean start = false;

    private GargleConfig.KafkaConsumerConfig consumerConfig;

    protected GargleConfig gargleConfig;

    protected ThreadPoolExecutor consumerExecutor;

    protected ThreadPoolExecutor processExecutor;

    protected BaseTask<K, V, R> task;

    @PostConstruct
    public synchronized void init() {
        if (start) {
            return;
        }
        start = true;
        gargleConfig = getGargleConfig();
        this.consumerConfig = getConsumerConfig();


        if (!consumerConfig.isEnable()) {
            logger.warn("{} 未开启.", log_prefix);
            return;
        }

        this.consumerExecutor = initConsumerPool();
        this.processExecutor = initProcessPool();
        start();
        log_prefix = "[kafka-consumer-client-" + getConsumerName() + "]-";
    }

    public void start() {
        logger.info("{}consumerName: {} kafka消费者开始启动,创建启动线程.", log_prefix, consumerConfig.getConsumerName());
        this.task = new BaseTask<>(consumerConfig, this);
        consumerExecutor.execute(task);
        // 钩子程序,释放资源
        // log4j 的 ShutdownHook 先于此 ShutdownHook 执行,导致这里面的日志不打印, 暂时使用 @PreDestroy 注解.
        /*Runtime.getRuntime().addShutdownHook(new Thread(consumerConfig.getConsumerName() + "-Shutdown") {
            @Override
            public void run() {
                close();
            }
        });*/

    }

    @PreDestroy
    public void close() {
        logger.info("{}consumerName: {} kafka消费者关闭中.", log_prefix, consumerConfig.getConsumerName());
        task.close();
        processExecutor.shutdown();
        consumerExecutor.shutdown();
        logger.info("{}consumerName: {} kafka消费者已关闭, 资源已释放.", log_prefix, consumerConfig.getConsumerName());
    }

    private ThreadPoolExecutor initConsumerPool() {
        return new ThreadPoolExecutor(
                1,
                1,
                1,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1),
                runnable -> new Thread(runnable, consumerConfig.getConsumerName())
        );
    }

    private ThreadPoolExecutor initProcessPool() {
        int size = consumerConfig.getConsumerMaxPollRecords();

        return new ThreadPoolExecutor(
                size,
                size * 2,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(size),
                runnable -> new Thread(
                        runnable,
                        consumerConfig.getConsumerName() + "-" + THREAD_SUFFIX.incrementAndGet()
                )
        );
    }

    private synchronized long getCount() {
        if (count.get() > 90_000_000) {
            count.set(0);
        }

        return count.getAndIncrement();
    }

    public static class BaseTask<K, V, R> implements Runnable, Serializable {

        private final GargleConfig.KafkaConsumerConfig consumerConfig;

        private final BaseConsumer<K, V, R> process;

        private volatile boolean run = true;

        private volatile boolean stop = false;

        public BaseTask(GargleConfig.KafkaConsumerConfig consumerConfig, BaseConsumer<K, V, R> process) {
            this.consumerConfig = consumerConfig;
            this.process = process;
        }

        @Override
        public void run() {
            Properties properties = consumerConfig.toConsumerProperties();
            KafkaConsumer<K, V> kafkaConsumer;
            try {
                kafkaConsumer = new KafkaConsumer<>(properties);
            } catch (Exception e) {
                logger.error("{}consumerName: {} kafka消费者创建异常.", log_prefix, consumerConfig.getConsumerName(), e);
                throw new GargleException(e);
            }

            try {
                kafkaConsumer.subscribe(Arrays.asList(consumerConfig.getTopics()));
            } catch (Exception e) {
                logger.error("{}consumerName: {} kafka消费者订阅topic异常.", log_prefix, consumerConfig.getConsumerName(), e);
                throw new GargleException(e);
            }

            logger.info("{}consumerName: {} kafka消费者启动线程创建完毕,已启动", log_prefix, consumerConfig.getConsumerName());
            long getTimeOut = consumerConfig.getConsumerMaxPollIntervalMs() / 3L * 2;
            long invokeTimeOut = consumerConfig.getConsumerMaxPollIntervalMs() / 3L;

            while (run) {
                long start = System.currentTimeMillis();
                final Boolean[] success = {true};
                try {
                    ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                    if (records != null && records.count() > 0) {
                        CopyOnWriteArrayList<Callable<R>> callables = new CopyOnWriteArrayList<>();
                        for (ConsumerRecord<K, V> record : records) {
                            Callable<R> callable = () -> {
                                String logPrefix = log_prefix + process.getCount() + "-";
                                try {
                                    return process.processRecord(record, logPrefix);
                                } catch (Exception e) {
                                    success[0] = false;
                                    process.onProcessRecordFail(record, e, logPrefix);
                                    return null;
                                }
                            };
                            callables.add(callable);
                        }

                        if (callables.size() != 0) {
                            List<Future<R>> futures;
                            futures = process.processExecutor.invokeAll(callables, invokeTimeOut, TimeUnit.MILLISECONDS);
                            for (Future<R> future : futures) {
                                R t;
                                try {
                                    t = future.get(getTimeOut, TimeUnit.MILLISECONDS);
                                } catch (TimeoutException e) {
                                    logger.warn("{}consumerName: {} 任务执行超时.", log_prefix, consumerConfig.getConsumerName());
                                    continue;
                                }
                                process.processT(t);
                            }
                            logger.info("{}consumerName: {} 本批次拉取: {} 条.", log_prefix, consumerConfig.getConsumerName(), records.count());
                        }
                        logger.info("{}consumerName: {} 本批次处理: {} 条记录, 耗时 {} ms", log_prefix, consumerConfig.getConsumerName(), records.count(), System.currentTimeMillis() - start);
                    }
                } catch (Exception e) {
                    success[0] = false;
                    logger.error("{}consumerName: {} kafka 消息执行异常", log_prefix, consumerConfig.getConsumerName(), e);
                }

                if (!consumerConfig.getEnableAutoCommit()) {
                    if (consumerConfig.getConsumerMaxPollRecords() == 1) {
                        if (success[0]) {
                            kafkaConsumer.commitSync();
                        }
                    } else {
                        kafkaConsumer.commitSync();
                    }
                }
            }

            try {
                kafkaConsumer.close();
            } catch (Exception e) {
                logger.error("{}consumerName: {} kafka关闭异常(可忽略), e: {}", log_prefix, consumerConfig.getConsumerName(), e);
            }
            stop = true;
            logger.info("{}consumerName: {} kafka消费者线程已执行完成, consumer链接已关闭.", log_prefix, consumerConfig.getConsumerName());
        }

        public void close() {
            run = false;
            int i = 0;
            Phaser phaser = new Phaser(1);
            // 等待线程执行完毕
            while (!stop) {
                try {
                    phaser.awaitAdvanceInterruptibly(phaser.getPhase(), 1, TimeUnit.SECONDS);
                } catch (InterruptedException | TimeoutException e) {
                    //ignore
                }
                i++;
                if (i >= 10) {
                    break;
                }
            }
        }
    }

    public GargleConfig.KafkaConsumerConfig getConsumerConfig() {
        String consumerName = getConsumerName();
        if (StringUtil.isBlank(consumerName)) {
            throw new GargleException("BaseConsumer-初始化存在consumerName为空的子类!");
        }

        List<GargleConfig.KafkaConsumerConfig> kafkaConsumers = gargleConfig.getKafkaConsumers();
        if (kafkaConsumers == null || kafkaConsumers.size() == 0) {
            throw new GargleException("BaseConsumer-初始化kafkaConsumers配置不存在 consumerName为: "
                    + consumerName + " 的kafka消费者配置. kafkaConsumers is null");
        }

        for (GargleConfig.KafkaConsumerConfig kafkaConsumer : kafkaConsumers) {
            if (kafkaConsumer == null) {
                continue;
            }

            if (consumerName.equals(kafkaConsumer.getConsumerName())) {
                return kafkaConsumer;
            }
        }

        throw new GargleException("BaseConsumer-初始化kafkaConsumers配置不存在 consumerName为: "
                + consumerName + " 的kafka消费者配置.");
    }

    public abstract String getConsumerName();

    public abstract GargleConfig getGargleConfig();

    public abstract R processRecord(ConsumerRecord<K, V> record, String logPrefix);

    public abstract void onProcessRecordFail(ConsumerRecord<K, V> record, Exception e, String logPrefix);

    public abstract void processT(R t);

}