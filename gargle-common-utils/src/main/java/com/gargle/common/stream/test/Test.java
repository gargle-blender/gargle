package com.gargle.common.stream.test;

import com.alibaba.fastjson.JSONObject;
import com.gargle.common.stream.client.StreamConsumerClient;
import com.gargle.common.stream.test.entity.MyEntity;
import com.gargle.common.stream.test.link.MyLinkExecute;

import java.util.concurrent.atomic.AtomicLong;

/**
 * ClassName:Test
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/20 13:56
 */
public class Test {

    public static void main(String[] args) {
        // 要操作的流程对象信息
        MyEntity myEntity = new MyEntity();
        myEntity.setName("sqw");
        myEntity.setAge(new AtomicLong(0));
        String recode = JSONObject.toJSONString(myEntity);

        // 初始化构建执行器
        MyLinkExecute execute = new MyLinkExecute();
        execute.init();

        // 初始化client
        StreamConsumerClient<String> consumerClient = () -> execute;

        // 将流程对象信息封装开始处理.
        consumerClient.process(recode);


    }
}
