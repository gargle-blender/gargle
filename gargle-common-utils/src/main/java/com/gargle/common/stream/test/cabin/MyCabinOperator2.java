package com.gargle.common.stream.test.cabin;

import com.gargle.common.stream.annotation.Cabin;
import com.gargle.common.stream.context.StreamContext;
import com.gargle.common.stream.operator.cabin.CabinOperator;
import com.gargle.common.stream.result.StreamResult;
import com.gargle.common.stream.test.entity.MyEntity;

/**
 * ClassName:MyCabinOperator1
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/20 14:12
 */
@Cabin(cabinNodeName = "node1", step = "step2", parallelism = 101)
public class MyCabinOperator2 extends CabinOperator<MyEntity> {

    @Override
    protected StreamResult handler(StreamContext<MyEntity> streamContext) {
        System.out.println(MSG + ": 执行");
        streamContext.getEntity().add1();
        return StreamResult.success();
    }
}
