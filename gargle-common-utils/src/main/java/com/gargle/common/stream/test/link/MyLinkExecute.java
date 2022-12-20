package com.gargle.common.stream.test.link;

import com.gargle.common.config.GargleConfig;
import com.gargle.common.enumeration.stream.StreamModeEnum;
import com.gargle.common.stream.link.AbstractStreamLinkExecute;
import com.gargle.common.stream.link.node.Node;
import com.gargle.common.stream.operator.base.BaseOperator;
import com.gargle.common.stream.test.cabin.MyCabinOperator1;
import com.gargle.common.stream.test.cabin.MyCabinOperator2;
import com.gargle.common.stream.test.cabin.MyCabinOperator3;
import com.gargle.common.stream.test.cabin.MyCabinOperator3R;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ClassName:MyLink
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/20 14:21
 */
public class MyLinkExecute extends AbstractStreamLinkExecute {

    @PostConstruct
    public void init() {
        super.init();
    }

    @Override
    protected Set<String> excludeNode() {
        return null;
    }

    @Override
    protected Set<String> excludeStep() {
        HashSet<String> set = new HashSet<>();
//        set.add("step1");
        return set;
    }

    @Override
    protected Set<String> containsOnlyNode() {
        HashSet<String> set = new HashSet<>();
//        set.add("node2");
        return set;
    }

    @Override
    protected List<BaseOperator<?>> operators() {
        ArrayList<BaseOperator<?>> operators = new ArrayList<>();
        /*MyLocomotiveOperator myLocomotiveOperator = new MyLocomotiveOperator();
        myLocomotiveOperator.init();
        operators.add(myLocomotiveOperator)*/
        MyCabinOperator1 myCabinOperator1 = new MyCabinOperator1();
        myCabinOperator1.init();
        MyCabinOperator2 myCabinOperator2 = new MyCabinOperator2();
        myCabinOperator2.init();
        MyCabinOperator3 myCabinOperator3 = new MyCabinOperator3();
        myCabinOperator3.init();
        MyCabinOperator3R myCabinOperator3R = new MyCabinOperator3R();
        myCabinOperator3R.init();
        ;


        operators.add(myCabinOperator1);
        operators.add(myCabinOperator2);
        operators.add(myCabinOperator3);
        operators.add(myCabinOperator3R);
        return operators;
    }

    @Override
    public List<Node> getNodeList() {
        ArrayList<Node> nodes = new ArrayList<>();
        /*nodes.add(new Node("node1", "node2"));
        nodes.add(new Node("node2", ""));*/
        return nodes;
    }

    @Override
    protected GargleConfig getGargleConfig() {
        GargleConfig gargleConfig = new GargleConfig();
        gargleConfig.setCabinStreamLinkNodes(new String[]{"node1", "node2"});
        return gargleConfig;
    }

    @Override
    protected StreamModeEnum getStreamMode() {
        return StreamModeEnum.CABIN;
    }

    @Override
    protected String getModeIsCabinFirstNodeName() {
        return "node1";
    }
}
