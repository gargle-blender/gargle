package com.gargle.common.config;

import com.alibaba.druid.filter.config.ConfigTools;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import com.gargle.common.enumeration.kafka.AutoOffsetResetEnum;
import com.gargle.common.exception.GargleException;
import com.gargle.common.utils.config.ConfigUtil;
import com.gargle.common.utils.hdfs.HDFSUtil;
import com.gargle.common.utils.sftp.SFTPUtil;
import com.gargle.common.utils.snow.SnowFlak;
import com.gargle.common.utils.string.StringUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ClassName:GargleConfig
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/11/29 15:06
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class GargleConfig {

    private static final Logger logger = LoggerFactory.getLogger(GargleConfig.class);

    protected SnowFlakConfig snow = new SnowFlakConfig();

    protected SFTPConfig sftp = new SFTPConfig();

    protected HDFSConfig hdfs = new HDFSConfig();

    protected DatasourceConfig datasource = new DatasourceConfig();

    protected List<KafkaConsumerConfig> kafkaConsumers = new ArrayList<>();

    protected List<KafkaProducerConfig> kafkaProducers = new ArrayList<>();

    @PostConstruct
    public void init() {
        if (datasource.isEnable()) {
            datasource.setConnectionProperties(ConfigUtil.buildConnectionProperties(datasource));
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class HDFSConfig {
        private boolean enable = false;

        /**
         * hdfs 连接
         */
        private String hdfsUrl;

        /**
         * 格式: k1=v1;k2=v2......
         */
        private String otherConfProperties;

        private String user;

        public HDFSUtil getSnowFlak() {
            if (!enable) {
                return null;
            }

            HDFSUtil.getInstance().init(this.hdfsUrl, ConfigUtil.buildProperties(otherConfProperties), this.getUser());
            return HDFSUtil.getInstance();
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SnowFlakConfig {
        private boolean enable = false;

        /**
         * 工作机器id  5bit  32
         */
        private Long workerId = 0L;

        /**
         * 5bit  32
         */
        private Long datacenterId = 0L;

        /**
         * 12bit 序列号  4096
         */
        private Long sequence = 0L;

        public SnowFlak getSnowFlak() {
            if (!enable) {
                return null;
            }
            SnowFlak.getInstance().setDatacenterId(this.getDatacenterId());
            SnowFlak.getInstance().setWorkerId(this.getWorkerId());
            SnowFlak.getInstance().setSequence(this.getSequence());
            SnowFlak.getInstance().init();
            return SnowFlak.getInstance();
        }
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SFTPConfig {
        private boolean enable = false;

        /**
         * FTP 登录用户名
         */
        private String username;
        /**
         * FTP 登录密码
         */
        private String password;
        /**
         * 私钥
         */
        private String privateKey;
        /**
         * FTP 服务器地址IP地址
         */
        private String host;
        /**
         * FTP 端口
         */
        private int port;

        public SFTPUtil buildSFTPUtil() {
            if (!enable) {
                return null;
            }
            SFTPUtil sftpUtil;
            try {
                if (StringUtil.isNotBlank(this.getPrivateKey())) {
                    sftpUtil = new SFTPUtil(
                            this.getUsername(),
                            this.getHost(),
                            this.getPort(),
                            this.getPrivateKey());
                } else {
                    sftpUtil = new SFTPUtil(
                            this.getUsername(),
                            this.getPassword(),
                            this.getHost(),
                            this.getPort());
                }
                sftpUtil.login();
                sftpUtil.logout();
            } catch (Exception e) {
                logger.error("sftp服务器配置预连接异常! 配置: {}", JSONObject.toJSONString(this), e);
                throw new GargleException("sftp服务器配置预连接异常!", e);
            }
            return sftpUtil;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DatasourceConfig {

        /**
         * 开关
         */
        private boolean enable = false;

        /**
         * 数据库链接url
         */
        private String url;

        /**
         * 数据库用户名
         */
        private String username;

        /**
         * 数据库密码,可加密
         */
        private String password;

        /**
         * 加密时的 publicKey
         */
        private String publicKey;

        /**
         * 数据库驱动
         */
        private String driverClassName;

        /**
         * mapper扫描路径
         */
        private String mapperScanPath;

        /**
         * 连接池配置信息，初始化连接数
         */
        private Integer initialSize = 5;

        /**
         * 连接池配置信息，最小连接数
         */
        private Integer minIdle = 5;

        /**
         * 连接池配置信息，最大连接数
         */
        private Integer maxActive = 20;

        /**
         * 配置获取连接等待超时的时间
         */
        private Long maxWait = 60_000L;

        /**
         * 配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
         */
        private Long timeBetweenEvictionRunsMillis = 60_000L;

        /**
         * 配置一个连接在池中最小生存的时间，单位是毫秒
         */
        private Long minEvictableIdleTimeMillis = 300_000L;

        /**
         * 验证数据库是否链接
         */
        private String validationQuery = "SELECT 1 FROM DUAL";

        /**
         * 当链接空闲时，是否测试链接可用性
         */
        private boolean testWhileIdle = true;

        /**
         * 当从连接池拿到连接时，是否测试连接的可用性
         */
        private boolean testOnBorrow = false;

        /**
         * 当链接归还连接池时，是否测试链接可用性
         */
        private boolean testOnReturn = false;

        /**
         * 控制PSCache（内存占用优化，大幅度提升sql执行性能，支持oracle，db2，sql server,不支持mysql）
         */
        private boolean poolPreparedStatements = false;

        /**
         * 指定每个连接上PSCache的大小
         */
        private Integer maxPoolPreparedStatementPerConnectionSize = 20;

        /**
         * 配置监控统计拦截的filters，去掉后监控界面sql无法统计，'wall'用于防火墙
         */
        private String filters = "config,stat,wall,log4j";

        /**
         * 通过connectProperties属性来打开mergeSql功能；慢SQL记录, 默认密码不加密,加密需要重新配置.
         */
        private String connectionProperties;

        /**
         * 开启慢sql查询.
         * 若 connectionProperties 中有此配置, 则connectionProperties配置优先级大于此配置
         * 对应配置项: druid.stat.logSlowSql
         */
        private boolean logSlowSql = true;

        /**
         * 开启慢sql记录时, 下限时间.
         * 若 connectionProperties 中有此配置, 则connectionProperties配置优先级大于此配置
         * 对应配置项: druid.stat.slowSqlMillis
         */
        private long slowSqlMillis = 5000L;

        /**
         * 密码是否加密
         * 若 connectionProperties 中有此配置, 则connectionProperties配置优先级大于此配置
         * 对应配置项: config.decrpt
         */
        private boolean decrpt = false;

        /**
         * 打开mergeSql功能
         * 若 connectionProperties 中有此配置, 则connectionProperties配置优先级大于此配置
         * 对应配置项: druid.stat.mergeSql
         */
        private boolean mergeSql = false;

        public DataSource buildDataSource() {
            DruidDataSource datasource = new DruidDataSource();
            datasource.setUrl(this.getUrl());
            datasource.setUsername(this.getUsername());
            if (this.isDecrpt()) {
                try {
                    datasource.setPassword(ConfigTools.decrypt(this.getPublicKey(), this.getPassword()));
                } catch (Exception e) {
                    throw new GargleException("druid configuration password error", e);
                }
            } else {
                datasource.setPassword(this.getPassword());
            }

            datasource.setDriverClassName(this.getDriverClassName());
            datasource.setInitialSize(this.getInitialSize());
            datasource.setMinIdle(this.getMinIdle());
            datasource.setMaxActive(this.getMaxActive());
            datasource.setMaxWait(this.getMaxWait());
            datasource.setTimeBetweenEvictionRunsMillis(this.getTimeBetweenEvictionRunsMillis());
            datasource.setMinEvictableIdleTimeMillis(this.getMinEvictableIdleTimeMillis());
            datasource.setValidationQuery(this.getValidationQuery());
            datasource.setTestWhileIdle(this.isTestWhileIdle());
            datasource.setTestOnBorrow(this.isTestOnBorrow());
            datasource.setTestOnReturn(this.isTestOnReturn());
            datasource.setConnectionProperties(this.getConnectionProperties());
            if (this.isPoolPreparedStatements()) {
                datasource.setPoolPreparedStatements(true);
                datasource.setMaxPoolPreparedStatementPerConnectionSize(
                        this.getMaxPoolPreparedStatementPerConnectionSize()
                );
            }
            try {
                datasource.setFilters(this.getFilters());
            } catch (SQLException e) {
                logger.error("druid configuration initialization filter:{}", this.getFilters(), e);
                throw new GargleException("druid configuration initialization filter: " + this.getFilters(), e);
            }
            return datasource;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class KafkaConsumerConfig {

        public KafkaConsumerConfig(String consumerName) {
            this.consumerName = consumerName;
        }

        private String consumerName;

        private String groupId;

        private String[] topics;

        private String topicPrefix;

        private String bootstrapServers;

        private boolean enable = false;

        private Boolean enableAutoCommit = true;

        private Integer consumerAutoCommitIntervalMs = 5000;

        private String consumerKeyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

        private String consumerValueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

        private Integer consumerSessionTimeoutMs = 60_000;

        private Integer consumerMaxPollIntervalMs = 300_000;

        private Integer consumerMaxPollRecords = 50;

        private Integer consumerHeartbeatIntervalMs = 3_000;

        private Integer consumerConnectionsMaxIdleMs = 300_000;

        private AutoOffsetResetEnum consumerAutoOffsetReset = AutoOffsetResetEnum.EARLIEST;

        public Properties toConsumerProperties() {
            Properties p = new Properties();
            ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();
            buildMap(map);
            p.putAll(map);
            return p;
        }

        public Map<String, Object> toOtherConfig() {
            ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();
            buildMap(map);
            return map;
        }

        public void buildMap(Map<String, Object> map) {
            if (consumerName == null) {
                throw new GargleException("KafkaConfig 缺少 [consumerName] 配置");
            }

            if (null == bootstrapServers) {
                throw new GargleException("[" + consumerName + "] 的 bootstrap.servers:bootstrapServers 未配置.");
            }

            if (null == groupId) {
                throw new GargleException("[" + consumerName + "] 的 group.id:groupId 未配置.");
            }

            if (null == topics) {
                throw new GargleException("[" + consumerName + "] 的 topic 未配置.");
            }
            map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
            map.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
            map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.consumerAutoOffsetReset.name().toLowerCase(Locale.ROOT));
            map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.consumerKeyDeserializer);
            map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.consumerValueDeserializer);
            map.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, this.consumerMaxPollIntervalMs);
            map.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, this.enableAutoCommit);
            map.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.consumerMaxPollRecords);
            map.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, this.consumerAutoCommitIntervalMs);
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class KafkaProducerConfig {

        private boolean enable = false;

        private String producerName;

        private String bootstrapServers;

        /**
         * topic
         * 多个逗号隔开
         */
        String[] topics;

        String clientId;

        /**
         * key序列化
         */
        String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";

        /**
         * value序列化
         */
        String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

        /**
         * ack方式
         */
        String acks = "1";

        /**
         * 压缩方式
         */
        String compressionType = "gzip";

        /**
         * 重试次数
         */
        Integer retries = 2;

        /**
         * 批处理大小
         */
        Integer batchSize = 5242880;

        /**
         * lingerMs
         */
        Integer lingerMs = 50;

        /**
         * 请求的最大字节数，要小于 message.max.bytes.
         */
        Integer maxRequestSize = 6291456;

        Integer bufferMemory = 6291456;

        Integer requestTimeoutMs = 300_000;

        public Properties buildProperties() {
            if (!enable) {
                return null;
            }
            Properties properties = new Properties();
            if (StringUtil.isBlank(this.bootstrapServers)) {
                throw new GargleException("[KafkaProducerConfig] bootstrap.servers 配置缺失.");
            }

            if (StringUtil.isNotBlank(this.clientId)) {
                properties.put(ProducerConfig.CLIENT_ID_CONFIG, this.clientId);
            }

            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.keySerializer);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.valueSerializer);
            properties.put(ProducerConfig.ACKS_CONFIG, this.acks);
            properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, this.compressionType);
            properties.put(ProducerConfig.RETRIES_CONFIG, this.retries);
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, this.batchSize);
            properties.put(ProducerConfig.LINGER_MS_CONFIG, this.lingerMs);
            properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, this.bufferMemory);
            properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, this.requestTimeoutMs);

            return properties;
        }

    }
}
