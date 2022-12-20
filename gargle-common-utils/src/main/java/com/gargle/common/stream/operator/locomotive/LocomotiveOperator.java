package com.gargle.common.stream.operator.locomotive;

import com.gargle.common.enumeration.stream.StreamCodeEnum;
import com.gargle.common.stream.annotation.Locomotive;
import com.gargle.common.stream.context.StreamContext;
import com.gargle.common.stream.context.base.BaseConvertEntity;
import com.gargle.common.stream.context.base.BaseEntity;
import com.gargle.common.stream.link.AbstractStreamLinkExecute;
import com.gargle.common.stream.link.node.Node;
import com.gargle.common.stream.operator.base.BaseOperator;
import com.gargle.common.stream.result.StreamResult;
import com.gargle.common.utils.string.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClassName:LocomotiveOperator
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/05 16:01
 */
public abstract class LocomotiveOperator<T extends BaseConvertEntity, RECODE> extends BaseOperator<T> {

    private static final Logger logger = LoggerFactory.getLogger(LocomotiveOperator.class);

    public abstract T buildT(RECODE recode);

    @Override
    protected StreamResult handler(StreamContext<T> streamContext) {
        if (streamContext == null) {
            return StreamResult.fail(new NullPointerException("DataLandContext is null."), "context为空!", StreamCodeEnum.CODE_FAIL_NO_RESEND);
        }

        if (streamContext.getRecode() == null) {
            return StreamResult.fail(new NullPointerException("message is null."), "message为空!", StreamCodeEnum.CODE_FAIL_NO_RESEND);
        }
        try {
            RECODE recode;
            if (streamContext.getRecode() == null) {
                logger.error("{}-消息为空!", getNode());
                // 没有实体需要处理则结束.
                return StreamResult.end();
            } else {
                recode = (RECODE) streamContext.getRecode();
            }
            T t = buildT(recode);
            if (t == null) {
                logger.error("{}-消息转换实体类为空!", getNode());
                // 没有实体需要处理则结束.
                return StreamResult.end();
            }
            streamContext.setEntity(t);
        } catch (Exception e) {
            return StreamResult.fail(e, "消息转换实体类异常!", StreamCodeEnum.CODE_FAIL_NO_RESEND);
        }
        return StreamResult.success();
    }

    @Override
    public StreamResult execute(StreamContext<T> streamContext) {
        String linkPath = node + "_" + step;
        // 跳过不执行
        if (skip || streamContext.getNode().isSkip()) {
            streamContext.getSkipLink().add(linkPath);
            return StreamResult.success();
        }

        if (streamContext.getLink().contains(linkPath)) {
            return StreamResult.fail(MSG + "[回环链路], 已执行: " + streamContext.getLink() + " 当前执行: " + linkPath);
        }

        streamContext.getLink().add(linkPath);

        // 判断当前 handler 是否执行.
        if (node.equals(streamContext.getNode().getNodeName())) {
            try {
                StreamResult streamResult = handler(streamContext);
                if (!streamResult.isSuccess() || streamResult.getCode().equals(StreamCodeEnum.CODE_END.getCode())) {
                    return streamResult;
                }
                String nextNodeName = nextNodeName(streamContext);
                if (StringUtil.isBlank(nextNodeName)) {
                    streamResult.setMessage(StreamCodeEnum.CODE_END.getMsg());
                    streamResult.setCode(StreamCodeEnum.CODE_END.getCode());
                    return streamResult;
                }
                if (!AbstractStreamLinkExecute.getNodeCache().containsKey(nextNodeName)) {
                    logger.warn(MSG + " nodeCacheMap 不包含 nextNodeName: " + nextNodeName);
                    streamResult.setMessage(MSG + " nodeCacheMap 不包含 nextNodeName: " + nextNodeName);
                    streamResult.setCode(StreamCodeEnum.CODE_END.getCode());
                    return streamResult;
                }
                Node node = AbstractStreamLinkExecute.getNodeCache().get(nextNodeName);
                if (node == null) {
                    logger.warn(MSG + " nodeCacheMap 缓存 nextNodeName 对应node对象为空: " + nextNodeName);
                    streamResult.setMessage(MSG + " nodeCacheMap 缓存 nextNodeName 对应node对象为空: " + nextNodeName);
                    streamResult.setCode(StreamCodeEnum.CODE_END.getCode());
                    return streamResult;
                }
                streamContext.getNode().setNextNode(node.getNodeName());
                return streamResult;
            } catch (Exception e) {
                return StreamResult.fail(e, MSG + " 执行异常, " + e.getMessage());
            }
        }

        //不执行
        return StreamResult.success();
    }

    @Override
    public String getNode() {
        if (this.getClass().getAnnotation(Locomotive.class) == null) {
            return null;
        }
        return this.getClass().getAnnotation(Locomotive.class).locomotiveNodeName();
    }

    @Override
    public String getStep() {
        if (this.getClass().getAnnotation(Locomotive.class) == null) {
            return null;
        }
        return this.getClass().getAnnotation(Locomotive.class).step();
    }

    @Override
    public Integer getParallelism() {
        return 100;
    }

    @Override
    public Boolean getSkip() {
        return false;
    }

    @Override
    public Double getOrder() {
        if (this.getClass().getAnnotation(Locomotive.class) == null) {
            return null;
        }
        return this.getClass().getAnnotation(Locomotive.class).order();
    }

    @Override
    public Class<? extends BaseEntity> getEntityClass() {
        return BaseEntity.class;
    }

    private String nextNodeName(StreamContext<T> streamContext) {
        return streamContext.getEntity().getFirstCabinNodeName();
    }
}
