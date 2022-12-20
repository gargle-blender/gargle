package com.gargle.common.stream.test.locomotive;

import com.alibaba.fastjson.JSONObject;
import com.gargle.common.stream.annotation.Locomotive;
import com.gargle.common.stream.operator.locomotive.LocomotiveOperator;
import com.gargle.common.stream.test.entity.MyEntity;
import com.gargle.common.utils.string.StringUtil;

/**
 * ClassName:MyLocomotiveOperator
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/20 14:01
 */
@Locomotive(locomotiveNodeName = "MyLocomotiveOperator")
public class MyLocomotiveOperator extends LocomotiveOperator<MyEntity, String> {

    @Override
    public MyEntity buildT(String s) {
        if (StringUtil.isBlank(s)) {
            return null;
        }
        return JSONObject.parseObject(s, MyEntity.class);
    }
}
