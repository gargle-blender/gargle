package com.gargle.common.stream.operator.cabin;

import com.gargle.common.stream.annotation.Cabin;
import com.gargle.common.stream.context.StreamContext;
import com.gargle.common.stream.context.base.BaseConvertEntity;
import com.gargle.common.stream.context.base.BaseEntity;
import com.gargle.common.stream.link.node.Node;
import com.gargle.common.stream.operator.base.BaseOperator;
import com.gargle.common.stream.result.StreamResult;

/**
 * ClassName:CabinOperator
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/05 15:58
 */
public abstract class CabinOperator<T extends BaseConvertEntity> extends BaseOperator<T> {

    @Override
    public StreamResult execute(StreamContext<T> streamContext) {
        Node node = streamContext.getNode();
        String linkPath = this.node + "_" + step;
        // 跳过: 外部配置优先级 > 内部代码优先级
        // 外部配置跳过, 则跳过不执行,
        if (node.isSkip()) {
            streamContext.getSkipLink().add(linkPath);
            return StreamResult.success();
        }

        if (streamContext.getLink().contains(linkPath)) {
            return StreamResult.fail(MSG + "[回环链路], 已执行: " + streamContext.getLink() + " 当前执行: " + linkPath);
        }

        streamContext.getLink().add(linkPath);

        if (this.node.equals(streamContext.getNode().getNodeName())) {
            try {
                StreamResult streamResult = handler(streamContext);
                if (streamResult == null) {
                    return StreamResult.fail(MSG + "handler 返回 StreamResult 为null! 请及时排查");
                }
                return streamResult;
            } catch (Exception e) {
                return StreamResult.fail(e, MSG + " handler 处理异常, " + e.getMessage());
            }
        }

        return StreamResult.success();
    }

    @Override
    public String getNode() {
        if (this.getClass().getAnnotation(Cabin.class) == null) {
            return null;
        }
        return this.getClass().getAnnotation(Cabin.class).cabinNodeName();
    }

    @Override
    public String getStep() {
        if (this.getClass().getAnnotation(Cabin.class) == null) {
            return null;
        }
        return this.getClass().getAnnotation(Cabin.class).step();
    }

    @Override
    public Integer getParallelism() {
        if (this.getClass().getAnnotation(Cabin.class) == null) {
            return null;
        }
        return this.getClass().getAnnotation(Cabin.class).parallelism();
    }

    @Override
    public Boolean getSkip() {
        if (this.getClass().getAnnotation(Cabin.class) == null) {
            return null;
        }
        return this.getClass().getAnnotation(Cabin.class).skip();
    }

    @Override
    public Double getOrder() {
        if (this.getClass().getAnnotation(Cabin.class) == null) {
            return null;
        }
        return this.getClass().getAnnotation(Cabin.class).order();
    }

    @Override
    public Class<? extends BaseEntity> getEntityClass() {
        if (this.getClass().getAnnotation(Cabin.class) == null) {
            return null;
        }
        return this.getClass().getAnnotation(Cabin.class).entityClass();
    }
}
