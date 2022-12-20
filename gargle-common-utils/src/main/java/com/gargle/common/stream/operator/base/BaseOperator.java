package com.gargle.common.stream.operator.base;

import com.gargle.common.serializable.Serializable;
import com.gargle.common.stream.context.StreamContext;
import com.gargle.common.stream.context.base.BaseConvertEntity;
import com.gargle.common.stream.context.base.BaseEntity;
import com.gargle.common.stream.result.StreamResult;

import javax.annotation.PostConstruct;

/**
 * ClassName:BaseOperator
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/05 15:52
 */
public abstract class BaseOperator<T extends BaseConvertEntity> implements Serializable {
    public String MSG;

    public String node;

    public String step;

    public Integer parallelism;

    public boolean skip;

    public double order;

    @PostConstruct
    public void init() {
        node = getNode();
        step = getStep();
        parallelism = getParallelism();
        skip = getSkip();
        order = getOrder();
        StringBuilder stringBuilder = new StringBuilder();
        StringBuilder append = stringBuilder.append("Node: ").append(node)
                .append(", Step: ").append(step)
                .append(", parallelism: ").append(parallelism)
                .append(", order: ").append(order)
                .append(", skip: ").append(skip).append(": ");
        MSG = append.toString();
    }

    protected abstract StreamResult handler(StreamContext<T> streamContext);

    public abstract StreamResult execute(StreamContext<T> streamContext);

    public abstract String getNode();

    public abstract String getStep();

    public abstract Integer getParallelism();

    public abstract Boolean getSkip();

    public abstract Double getOrder();

    public abstract Class<? extends BaseEntity> getEntityClass();
}
