package com.gargle.common.utils.snow;

import com.gargle.common.exception.GargleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClassName:SnowFlak
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/04/28 15:26
 */
public final class SnowFlak {

    private static final Logger logger = LoggerFactory.getLogger(SnowFlak.class);

    private static final String log = "[SnowFlak]";

    /**
     * 初始时间值,一旦使用后,不允许该更大的值.否则会导致id重复.
     */
    private static final long initTimestamp = 1651132344840L;

    /**
     * 长度为5位
     */
    private static final long workerIdBits = 5L;

    /**
     * 数据中心id为 5 位.
     */
    private static final long datacenterIdBits = 5L;

    /**
     * 序列号id长度
     */
    private static final long sequenceBits = 12L;

    /**
     * 最大值 -1 左移 5，得结果a，-1 异或 a：利用位运算计算出5位能表示的最大正整数是多少。
     */
    private static final long maxWorkerId = ~(-1L << workerIdBits); //31

    private static final long maxDatacenterId = ~(-1L << datacenterIdBits); // 31

    /**
     * 序列号最大值
     */
    private static final long sequenceMask = ~(-1L << sequenceBits); //4095

    /**
     * workerId需要左移的位数，12位
     */
    private static final long workerIdShift = sequenceBits; //12

    /**
     * datacenterId需要左移位数
     */
    private static final long datacenterIdShift = sequenceBits + workerIdBits; // 12+5=17

    /**
     * 时间戳需要左移位数
     */
    private static final long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits; // 12+5+5=22

    /**
     * 是否初始化, 未初始化不可用!
     */
    private static boolean init = false;

    /**
     * 工作机器id  5bit  32
     */
    private Long workerId;

    /**
     * 5bit  32
     */
    private Long datacenterId;

    /**
     * 12bit 序列号  4096
     */
    private Long sequence;

    //上次时间戳，初始值为负数
    private long lastTimestamp = -1L;

    public void init() {
        if (workerId == null) {
            throw new NullPointerException(log + " workerId 属性缺失.");
        }

        if (datacenterId == null) {
            throw new NullPointerException(log + " datacenterId 属性缺失.");
        }

        if (sequence == null) {
            throw new NullPointerException(log + " sequence 属性缺失.");
        }

        if (workerId > maxWorkerId || workerId < 0) {
            throw new GargleException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
        }
        if (datacenterId > maxDatacenterId || datacenterId < 0) {
            throw new GargleException(String.format("datacenter Id can't be greater than %d or less than 0", maxDatacenterId));
        }
        logger.info("worker starting. timestamp left shift {}, datacenter id bits {}, worker id bits {}, sequence bits {}, workerid {}",
                timestampLeftShift, datacenterIdBits, workerIdBits, sequenceBits, workerId);

        init = true;
    }

    /**
     * 下一个ID生成算法
     */
    public synchronized long nextId() {
        if (!init) {
            throw new NullPointerException(log + " 未初始化!");
        }

        long timestamp = timeGen();

        //获取当前时间戳如果小于上次时间戳，则表示时间戳获取出现异常
        if (timestamp < lastTimestamp) {
            System.err.printf("clock is moving backwards.  Rejecting requests until %d.", lastTimestamp);
            throw new GargleException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds",
                    lastTimestamp - timestamp));
        }

        //获取当前时间戳如果等于上次时间戳（同一毫秒内），则在序列号加一；否则序列号赋值为0，从0开始。
        if (lastTimestamp == timestamp) {
            // 通过位与运算保证计算的结果范围始终是 0-4095
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }

        //将上次时间戳值刷新
        lastTimestamp = timestamp;

        /*
         * 返回结果：
         * (timestamp - twepoch) << timestampLeftShift) 表示将时间戳减去初始时间戳，再左移相应位数
         * (datacenterId << datacenterIdShift) 表示将数据id左移相应位数
         * (workerId << workerIdShift) 表示将工作id左移相应位数
         * | 是按位或运算符，例如：x | y，只有当x，y都为0的时候结果才为0，其它情况结果都为1。
         * 因为个部分只有相应位上的值有意义，其它位上都是0，所以将各部分的值进行 | 运算就能得到最终拼接好的id
         */
        return ((timestamp - initTimestamp) << timestampLeftShift) |
                (datacenterId << datacenterIdShift) |
                (workerId << workerIdShift) |
                sequence;
    }

    /**
     * 获取时间戳，并与上次时间戳比较
     */
    private long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    /**
     * 获取系统时间戳
     */
    private long timeGen() {
        return System.currentTimeMillis();
    }


    public void setWorkerId(long workerId) {
        this.workerId = workerId;
    }

    public void setDatacenterId(long datacenterId) {
        this.datacenterId = datacenterId;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public static SnowFlak getInstance() {
        return Holder.snowFlak;
    }

    private static class Holder {
        public static final SnowFlak snowFlak = new SnowFlak();
    }

    public static void main(String[] args) {
        //126198886411
        System.out.println(System.currentTimeMillis());
        SnowFlak.getInstance().setDatacenterId(5);
        SnowFlak.getInstance().setWorkerId(3);
        SnowFlak.getInstance().setSequence(4);
        SnowFlak.getInstance().init();
        for (int i = 0; i < 100; i++) {
            System.out.println(SnowFlak.getInstance().nextId());
        }
    }
}
