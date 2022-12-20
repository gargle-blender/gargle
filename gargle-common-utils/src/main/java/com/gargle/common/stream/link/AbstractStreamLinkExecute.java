package com.gargle.common.stream.link;

import com.gargle.common.config.GargleConfig;
import com.gargle.common.enumeration.stream.StreamCodeEnum;
import com.gargle.common.enumeration.stream.StreamModeEnum;
import com.gargle.common.stream.annotation.Cabin;
import com.gargle.common.stream.context.StreamContext;
import com.gargle.common.stream.context.base.BaseConvertEntity;
import com.gargle.common.stream.context.base.BaseEntity;
import com.gargle.common.stream.link.base.StreamLinkExecute;
import com.gargle.common.stream.link.node.Node;
import com.gargle.common.stream.operator.base.BaseOperator;
import com.gargle.common.stream.operator.cabin.CabinOperator;
import com.gargle.common.stream.result.StreamResult;
import com.gargle.common.utils.string.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ClassName:AbstractStreamLinkExecute
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/05 14:52
 */
public abstract class AbstractStreamLinkExecute implements StreamLinkExecute<StreamContext<? extends BaseConvertEntity>> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractStreamLinkExecute.class);

    private static final String threadPrefix = "stream-";

    /**
     * node 和 node对应的步骤的映射.
     * 外部配置
     * key: node名称
     * value: steps 名称
     */
    protected static final Map<String, Set<String>> nodeStepCache = new HashMap<>();

    /**
     * key: node名称
     * value: 顺序-并行执行的Operator.
     */
    protected static final Map<String, List<List<BaseOperator<?>>>> operatorCache = new HashMap<>();

    /**
     * node 名称和对应node信息的映射.
     */
    protected static final Map<String, Node> nodeCache = new HashMap<>();

    /**
     * 处理步骤对应的class模板映射.
     */
    public static Map<String, Class<? extends BaseEntity>> stepClassCache = new HashMap<>();

    protected ThreadPoolExecutor executor = buildThreadPoolExecutor();

    @PostConstruct
    public void init() {
        loadLink();
    }

    public void loadLink() {
        logger.info("执行链路加载....");

        StreamModeEnum streamMode = getStreamMode();
        if (streamMode == null) {
            throw new NullPointerException("[执行链路加载]-StreamMode is null");
        }

        // 1.根据外部配置的 nodeList 构建 nodeCache.
        List<Node> nodeList = getNodeList(getGargleConfig() == null ? null : getGargleConfig().getCabinStreamLinkNodes());
        if (nodeList == null || nodeList.size() == 0) {
            throw new RuntimeException("[执行链路加载]-nodeList为空! ");
        }
        HashSet<String> allNodes = new HashSet<>();
        boolean end = false;
        for (Node node : nodeList) {
            if (node == null) {
                continue;
            }

            if (StringUtil.isBlank(node.getNodeName())) {
                throw new RuntimeException("[执行链路加载]-存在nodeName为空! ");
            }
            allNodes.add(node.getNodeName().trim());
            if (!StringUtil.isBlank(node.getNextNode())) {
                allNodes.add(node.getNextNode().trim());
            }
            nodeCache.put(node.getNodeName().trim(), node);
            if (StringUtil.isBlank(node.getNextNode())) {
                end = true;
            }
        }

        if (streamMode.name().equals(StreamModeEnum.CABIN.name())) {
            if (!end) {
                throw new IllegalArgumentException("[执行链路加载]-CABIN 模式无尾节点配置!");
            }
        }

        for (String nodeName : allNodes) {
            if (!nodeCache.containsKey(nodeName)) {
                nodeCache.put(nodeName, new Node(nodeName, null));
            }
        }

        // 2.获取外部配置的过滤节点和步骤.
        Set<String> excludeStep = buildSet(excludeStep());
        Set<String> excludeNode = buildSet(excludeNode());
        Set<String> containsOnlyNode = buildSet(containsOnlyNode());

        // 3.校验 excludeNode 和 containsOnlyNode 的值不能互相包含.
        HashSet<String> set = new HashSet<>();
        for (String exclude : excludeNode) {
            if (containsOnlyNode.contains(exclude)) {
                set.add(exclude);
            }
        }
        for (String contains : containsOnlyNode) {
            if (excludeNode.contains(contains)) {
                set.add(contains);
            }
        }
        if (set.size() > 0) {
            throw new RuntimeException("[执行链路加载]-excludeNode 和 containsOnlyNode 存在重复值: " + set);
        }

        // 4.containsOnlyNode 中的node必须在配置中存在!
        if (containsOnlyNode.size() > 0) {
            for (String nodeName : containsOnlyNode) {
                if (StringUtil.isBlank(nodeName)) {
                    continue;
                }
                if (!nodeCache.containsKey(nodeName.trim())) {
                    throw new RuntimeException("[执行链路加载]-执行节点配置: " + nodeName.trim() + " 不存在于所有节点中! all: " + nodeCache.keySet());
                }
            }

            for (Map.Entry<String, Node> entry : nodeCache.entrySet()) {
                if (!containsOnlyNode.contains(entry.getKey())) {
                    logger.warn("[执行链路加载]-nodeName: {} 不被执行节点配置包含, 被设置为跳过.", entry.getKey());
                    entry.getValue().setSkip(true);
                }
            }
        }

        // 5.排除的节点node设置全部跳过.
        if (excludeNode.size() > 0) {
            for (String exclude : excludeNode) {
                if (!nodeCache.containsKey(exclude)) {
                    logger.warn("[执行链路加载]-额外排除构建 nodeName: {}", exclude);
                    nodeCache.put(exclude, new Node(true, exclude, null));
                }
            }
        }

        // 6.获取所有实现类.
        List<BaseOperator<?>> beans = operators();
        if (beans == null || beans.size() < 1) {
            throw new NullPointerException("[执行链路加载]-无 [BaseOperator] 实现.");
        }

        // 7.根据注解的 Order 值对同一个step的多个实现类进行过滤.
        Map<String, BaseOperator<?>> filterMap = new ConcurrentHashMap<>();
        for (BaseOperator<?> operator : beans) {
            if (operator == null) {
                continue;
            }
            String node = operator.getNode();
            String step = operator.getStep();
            boolean skip = operator.getSkip();
            Double order = operator.getOrder();
            // 若注解获取值为空, 则跳过不构建
            if (StringUtil.isBlank(node) || StringUtil.isBlank(step) || order == null) {
                logger.warn("[执行链路加载]-BaseOperator存在注解值为空!");
                continue;
            }
            node = node.trim();
            step = step.trim();

            // 若 只包含节点配置不为空,且不包含此节点, 则跳过不构建.
            if (containsOnlyNode.size() > 0 && operator instanceof CabinOperator) {
                if (!containsOnlyNode.contains(node.trim())) {
                    continue;
                }
            }
            // 若 排除步骤包含此步骤, 则跳过不构建.
            if (excludeStep.contains(step.trim()) && operator instanceof CabinOperator) {
                continue;
            }
            // 若 除外节点包含此节点, 则跳过不构建.
            if (excludeNode.contains(node.trim()) && operator instanceof CabinOperator) {
                continue;
            }
            // 若 注解跳过此步骤, 则跳过不构建.
            if (skip) {
                continue;
            }

            // 获取 node 的 steps.
            if (nodeStepCache.containsKey(node)) {
                Set<String> steps = nodeStepCache.get(node);
                steps.add(step);
                nodeStepCache.put(node, steps);
            } else {
                Set<String> steps = new HashSet<>();
                steps.add(step);
                nodeStepCache.put(node, steps);
            }

            // 根据 order 大小进行替换. 生效的是 order 小的handler.
            String filterKey = getFilterKey(node, step);
            if (filterMap.containsKey(filterKey)) {
                BaseOperator<?> lastOperator = filterMap.get(filterKey);
                if (lastOperator.getOrder().equals(operator.getOrder())) {
                    throw new IllegalArgumentException("[执行链路加载]-filterKey: " + filterKey + " 存在两个 Order 相同的 CabinOperator");
                }

                if (lastOperator.getOrder() > operator.getOrder()) {
                    filterMap.put(filterKey, operator);
                }
                continue;
            }

            filterMap.put(filterKey, operator);
        }

        // 8.根据filterMap 和 注解的 parallelism 属性值进行 operatorCache 的排序封装.
        for (Map.Entry<String, BaseOperator<?>> operatorEntry : filterMap.entrySet()) {
            BaseOperator<?> operator = operatorEntry.getValue();
            String node = operator.getNode().trim();
            if (operatorCache.containsKey(node)) {
                List<List<BaseOperator<?>>> lists = operatorCache.get(node);
                boolean add = false;
                for (List<BaseOperator<?>> list : lists) {
                    if (list.get(0).getParallelism().equals(operator.getParallelism())) {
                        add = true;
                        list.add(operator);
                    }
                }

                if (!add) {
                    List<BaseOperator<?>> list = new ArrayList<>();
                    list.add(operator);
                    lists.add(list);
                }

                lists.sort((o1, o2) -> {
                    Integer parallelism1 = o1.get(0).getParallelism();
                    Integer parallelism2 = o2.get(0).getParallelism();
                    return parallelism1.compareTo(parallelism2);
                });
            } else {
                List<List<BaseOperator<?>>> lists = new ArrayList<>();
                List<BaseOperator<?>> list = new ArrayList<>();
                list.add(operator);
                lists.add(list);
                operatorCache.put(node, lists);
            }
        }

        // 9.operatorCache 中不存在的node设置为跳过. 因为没有实现类被加载.
        for (Map.Entry<String, Node> nodeEntry : nodeCache.entrySet()) {
            if (!nodeEntry.getValue().isSkip()) {
                if (!operatorCache.containsKey(nodeEntry.getValue().getNodeName().trim())) {
                    logger.warn("[执行链路加载]-nodeName"
                            + nodeEntry.getValue().getNodeName() + " 无 BaseOperator 实现类加载. 将跳过.");
                    nodeEntry.getValue().setSkip(true);
                }
            }
        }

        // 10.对 cabin 的step和转换的实体类模板进行封装缓存. stepClassCache
        for (Map.Entry<String, List<List<BaseOperator<?>>>> entry : operatorCache.entrySet()) {
            for (List<BaseOperator<?>> list : entry.getValue()) {
                for (BaseOperator<?> handler : list) {
                    if (handler instanceof CabinOperator) {
                        Cabin annotation = handler.getClass().getAnnotation(Cabin.class);
                        Class<? extends BaseEntity> value = annotation.entityClass();
                        String step = annotation.step();
                        if (value != BaseEntity.class) {
                            stepClassCache.put(step, value);
                        }
                    }
                }
            }
        }

        // 校验岔路执行. 和 回环链路.
        HashMap<String, String> map = new HashMap<>();
        for (Node node : nodeList) {
            if (map.containsKey(node.getNodeName())) {
                throw new IllegalArgumentException("岔路执行: " + node.getNodeName());
            } else {
                map.put(node.getNodeName(), node.getNextNode());
            }

            Node checkTempNode = new Node(node.getNodeName(), node.getNextNode());
            int i = 0;
            while (checkTempNode != null) {
                i++;
                checkTempNode = nodeCache.get(checkTempNode.getNextNode());
                if (i > nodeCache.size()) {
                    throw new IllegalArgumentException("链路校验失败! 请检查是否存在回环链路.");
                }
            }
        }
        if (streamMode.name().equals(StreamModeEnum.CABIN.name())) {
            String modeIsCabinFirstNodeName = getModeIsCabinFirstNodeName();
            if (StringUtil.isBlank(modeIsCabinFirstNodeName)) {
                throw new NullPointerException("[执行链路加载]-modeIsCabinFirstNodeName is null");
            }

            if (!nodeCache.containsKey(modeIsCabinFirstNodeName)) {
                throw new NullPointerException("[执行链路加载]-nodeCache 不包含 modeIsCabinFirstNodeName: " + modeIsCabinFirstNodeName);
            }

            if (!allNodes.contains(modeIsCabinFirstNodeName)) {
                throw new NullPointerException("[执行链路加载]-nodeList 不包含 modeIsCabinFirstNodeName: " + modeIsCabinFirstNodeName);
            }

            Node node = nodeCache.get(modeIsCabinFirstNodeName);

            int i = 0;
            while (node != null) {
                i++;
                node = nodeCache.get(node.getNextNode());
                if (i > nodeCache.size()) {
                    throw new IllegalArgumentException("链路校验失败! 请检查是否存在回环链路.");
                }
            }

            if (i != nodeList.size()) {
                throw new IllegalArgumentException("链路校验失败");
            }
        }


    }

    /**
     * 哪些节点不执行.
     */
    protected abstract Set<String> excludeNode();

    /**
     * 哪些步骤不执行
     */
    protected abstract Set<String> excludeStep();

    /**
     * 只执行哪些节点
     */
    protected abstract Set<String> containsOnlyNode();

    protected abstract List<BaseOperator<?>> operators();

    /**
     * 跳过配置: 外部配置优先级 > 这里构建 > 注解.
     */
    public List<Node> getNodeList(String[] cabinStreamLinkNode) {
        if (getStreamMode().name().equals(StreamModeEnum.CABIN.name())) {
            if (cabinStreamLinkNode == null) {
                return getNodeList();
            }
            ArrayList<Node> nodes = new ArrayList<>();
            for (int i = 1; i < cabinStreamLinkNode.length; i++) {
                nodes.add(new Node(cabinStreamLinkNode[i - 1], cabinStreamLinkNode[i]));
                if (i == cabinStreamLinkNode.length - 1) {
                    nodes.add(new Node(cabinStreamLinkNode[i], null));
                }
            }
            return nodes;
        } else {
            return getNodeList();
        }
    }

    public abstract List<Node> getNodeList();

    protected abstract GargleConfig getGargleConfig();

    protected StreamModeEnum getStreamMode() {
        GargleConfig gargleConfig = getGargleConfig();
        if (gargleConfig == null) {
            throw new NullPointerException("GargleConfig is null");
        }
        if (gargleConfig.getStreamMode() == null) {
            throw new NullPointerException("GargleConfig.StreamMode is null");
        }

        return gargleConfig.getStreamMode();
    }

    protected abstract String getModeIsCabinFirstNodeName();

    /**
     * 构建第一个执行的node.
     * CABIN: 单链路执行, 以配置链路为准.
     * LOCOMOTIVE: 多链路执行, streamContext 上下文封装为准.
     */
    @Override
    public StreamResult apply(StreamContext streamContext) {
        Node streamNode = streamContext.getNode();
        if (getStreamMode().name().equals(StreamModeEnum.CABIN.name())) {
            streamNode = nodeCache.get(getModeIsCabinFirstNodeName());
        }
        streamContext.setNode(streamNode);
        return execute(streamContext);
    }

    /**
     * 与 executeA 交替递归防止堆栈溢出.
     */
    private StreamResult execute(StreamContext streamContext) {
        String nodeName = streamContext.getNode().getNodeName();
        if (StringUtil.isBlank(nodeName)) {
            return StreamResult.fail("DataLandContext 存在空的 nodeName!");
        }

        List<List<BaseOperator<?>>> lists = operatorCache.get(nodeName);
        if (lists != null) {
            StreamResult streamResult = handler(lists, streamContext, nodeName);
            if (!streamResult.isSuccess() || StreamCodeEnum.CODE_END.getCode().equals(streamResult.getCode())) {
                return streamResult;
            }
        }
        Node node = null;
        if (!nodeCache.containsKey(nodeName)) {
            node = streamContext.getNode();
        } else {
            node = nodeCache.get(nodeName);
        }

        Node nextNode = buildContextNode(node.getNextNode());
        if (nextNode == null) {
            return StreamResult.end();
        }
        streamContext.setNode(nextNode);
        return executeA(streamContext);
    }

    private StreamResult executeA(StreamContext streamContext) {
        return execute(streamContext);
    }

    public StreamResult handler(List<List<BaseOperator<?>>> lists, StreamContext context, String nodeName) {
        StreamResult failResult = null;
        boolean end = false;
        for (List<BaseOperator<?>> list : lists) {
            if (list == null || list.size() < 1) {
                continue;
            }
            if (list.size() > 1) {
                ArrayList<Future<StreamResult>> futures = new ArrayList<>(list.size());
                for (BaseOperator<?> operator : list) {
                    Future<StreamResult> submit = executor.submit(new Callable<StreamResult>() {
                        @Override
                        public StreamResult call() throws Exception {
                            return operator.execute(context);
                        }
                    });
                    futures.add(submit);
                }


                if (futures.size() > 1) {
                    for (Future<StreamResult> future : futures) {
                        StreamResult streamResult = null;
                        try {
                            streamResult = future.get(30, TimeUnit.SECONDS);
                            if (StreamCodeEnum.CODE_END.getCode().equals(streamResult.getCode())) {
                                end = true;
                            }
                        } catch (Exception e) {
                            buildFailResult(StreamResult.fail(e, "NodeName: " + nodeName
                                            + ", handler 并发执行异常: " + e.getMessage()),
                                    failResult);
                            continue;
                        }
                        failResult = buildFailResult(streamResult, failResult);
                    }
                }
                continue;
            }

            StreamResult handleResult = list.get(0).execute(context);
            if (StreamCodeEnum.CODE_END.getCode().equals(handleResult.getCode())) {
                end = true;
            }
            failResult = buildFailResult(handleResult, failResult);
        }
        if (failResult != null) {
            return failResult;
        }

        if (end) {
            return StreamResult.end();
        }
        return StreamResult.success();
    }

    public static Map<String, Set<String>> getNodeStepCache() {
        return Collections.unmodifiableMap(nodeStepCache);
    }

    public static Map<String, List<List<BaseOperator<?>>>> getOperatorCache() {
        return Collections.unmodifiableMap(operatorCache);
    }

    public static Map<String, Node> getNodeCache() {
        return Collections.unmodifiableMap(nodeCache);
    }

    protected ThreadPoolExecutor getThreadPoolExecutor() {
        return null;
    }

    private ThreadPoolExecutor buildThreadPoolExecutor() {
        ThreadPoolExecutor threadPoolExecutor = getThreadPoolExecutor();
        if (threadPoolExecutor != null) {
            logger.warn("系统初始化流线程池, 使用外部加载线程池!");
            return threadPoolExecutor;
        }

        GargleConfig gargleConfig = getGargleConfig();
        if (gargleConfig == null) {
            logger.warn("系统初始化流线程池, GargleConfig 为空, 系统不初始化.");
            return null;
        }

        if (gargleConfig.getStreamThreadPoolConfig() == null) {
            logger.warn("系统初始化流线程池, StreamThreadPoolConfig 为空, 系统不初始化.");
            return null;
        }
        GargleConfig.StreamThreadPoolConfig poolConfig = gargleConfig.getStreamThreadPoolConfig();
        int coreSize = poolConfig.getCoreSize() == null ? Runtime.getRuntime().availableProcessors() : poolConfig.getCoreSize();
        int maxSize = poolConfig.getMaxSize() == null ? Runtime.getRuntime().availableProcessors() * 2 : poolConfig.getMaxSize();
        long keepAliveTime = poolConfig.getKeepAliveTime() == null ? 60 : poolConfig.getKeepAliveTime();
        int queueSize = poolConfig.getQueueSize() == null ? 2000 : poolConfig.getQueueSize();
        TimeUnit timeUnit = poolConfig.getTimeUnit() == null ? TimeUnit.SECONDS : poolConfig.getTimeUnit();

        logger.info("系统初始化流线程池, 参数如下: \n" +
                        "核心线程数: {}, \n" +
                        "最大线程数: {}, \n" +
                        "存活时间: {}, \n" +
                        "队列大小: {}, \n" +
                        "单位: {} \n",
                coreSize, maxSize, keepAliveTime, queueSize, timeUnit.name());
        return new ThreadPoolExecutor(coreSize, maxSize, keepAliveTime, timeUnit, new LinkedBlockingQueue<>(queueSize), new ThreadFactory() {

            private final AtomicLong count = new AtomicLong();

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, threadPrefix + count.incrementAndGet());
            }
        }, new ThreadPoolExecutor.CallerRunsPolicy() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                logger.warn("{} 线程池执行拒绝策略! 请及时排查优化!", threadPrefix);
                super.rejectedExecution(r, e);
            }
        });
    }

    private Set<String> buildSet(Set<String> set) {
        Set<String> hashSet = new HashSet<>();
        if (set == null) {
            return new HashSet<>();
        }

        for (String s : set) {
            hashSet.add(s.trim());
        }

        return hashSet;
    }

    private String getFilterKey(String node, String step) {
        return node + "$" + step;
    }

    private Node buildContextNode(String nodeName) {
        if (StringUtil.isBlank(nodeName)) {
            return null;
        }
        Node node = nodeCache.get(nodeName);
        if (node == null) {
            logger.warn("[HandlerCacheService]-nodeCache 不包含 node: {}", nodeName);
            return null;
        }

        if (node.isSkip()) {
            if (node.getNextNode() == null) {
                return null;
            }
            return buildContextNode(node.getNextNode());
        }

        return node;
    }

    private StreamResult buildFailResult(StreamResult streamResult, StreamResult failResult) {
        if (!streamResult.isSuccess()) {
            if (failResult == null) {
                failResult = streamResult;
            } else {
                failResult.getErrorMessages().add(streamResult.getMessage());
                if (streamResult.getExceptions().size() > 0) {
                    failResult.getExceptions().addAll(streamResult.getExceptions());
                }
                if (StreamCodeEnum.CODE_FAIL_NO_RESEND.getCode().equals(streamResult.getCode())) {
                    failResult.setCode(StreamCodeEnum.CODE_FAIL_NO_RESEND.getCode());
                }
            }
        }
        return failResult;
    }

    @PreDestroy
    public void destroy() {
        if (executor != null && !executor.isShutdown() && !executor.isTerminated()) {
            executor.shutdown();
            logger.warn("{}线程池等待关闭中, 预计时间 300 秒......", threadPrefix);
            try {
                boolean b = executor.awaitTermination(300, TimeUnit.SECONDS);
                if (!b) {
                    logger.error("{}线程池等待关闭失败", threadPrefix);
                }
            } catch (Exception e) {
                logger.error("{}线程池等待关闭异常", threadPrefix, e);
            }
        }
    }
}
