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
        MyEntity myEntity = new MyEntity();
        myEntity.setName("sqw");
        myEntity.setAge(new AtomicLong(0));
        String recode = JSONObject.toJSONString(myEntity);

        MyLinkExecute execute = new MyLinkExecute();
        execute.init();

        StreamConsumerClient<String> consumerClient = () -> execute;
        consumerClient.process(recode);


    }
}
