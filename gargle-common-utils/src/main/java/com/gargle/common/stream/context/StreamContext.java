package com.gargle.common.stream.context;

import com.gargle.common.enumeration.stream.SkipPolicyEnum;
import com.gargle.common.serializable.Serializable;
import com.gargle.common.stream.context.base.BaseConvertEntity;
import com.gargle.common.stream.context.base.BaseEntity;
import com.gargle.common.stream.link.node.Node;
import com.gargle.common.stream.result.StreamResult;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ClassName:StreamContext
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/05 14:55
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StreamContext<Entity extends BaseConvertEntity> implements Serializable {

    /**
     * 进件记录
     */
    private Object recode;

    /**
     * 进件记录转换的消息实体.
     */
    private Entity entity;

    /**
     * 当前执行节点信息.
     */
    private Node node;

    private SkipPolicyEnum skipPolicy = SkipPolicyEnum.EXTERIOR;

    /**
     * 已执行的流程
     */
    private Set<String> link = Collections.synchronizedSet(new LinkedHashSet<>(1000));

    /**
     * 被跳过的流程
     */
    private Set<String> skipLink = Collections.synchronizedSet(new LinkedHashSet<>(1000));

    /**
     * 流程结果信息. key: stepName  value:最终结果
     */
    private Map<String, List<BaseEntity>> result = new ConcurrentHashMap<>();

    /**
     * 处理结果.
     */
    private StreamResult streamResult;

    /**
     * locomotive模式调用.
     * cabin 模式在特殊情况下也可调用.
     */
    public StreamContext(Object recode, String firstNodeName) {
        this.recode = recode;
        this.node = new Node(firstNodeName, null);
    }

    /**
     * cabin模式调用
     */
    public StreamContext(Object recode) {
        this.recode = recode;
    }

    public boolean skip(boolean skip) {
        if (skipPolicy.name().equals(SkipPolicyEnum.ALL.name())) {
            return skip && node.isSkip();
        }

        if (skipPolicy.name().equals(SkipPolicyEnum.INTERIOR.name())) {
            return skip;
        }

        if (skipPolicy.name().equals(SkipPolicyEnum.EXTERIOR.name())) {
            return node.isSkip();
        }

        return true;
    }
}
