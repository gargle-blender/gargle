package com.gargle.common.stream.test.entity;

import com.gargle.common.stream.context.base.BaseConvertEntity;
import lombok.Data;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ClassName:MyEntity
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/20 13:57
 */
@Data
public class MyEntity implements BaseConvertEntity {

    private String node;

    private String name;

    private AtomicLong age = new AtomicLong(0);

    @Override
    public Map<String, Object> extParam() {
        return null;
    }

    @Override
    public String getFirstCabinNodeName() {
        return null;
    }

    public void add1() {
        age.incrementAndGet();
    }
}
