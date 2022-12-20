package com.gargle.common.stream.context.base;

import com.gargle.common.serializable.Serializable;

import java.util.Map;

/**
 * ClassName:BaseConvertEntity
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/05 15:18
 */
public interface BaseConvertEntity extends Serializable {

    Map<String, Object> extParam();

    String getFirstCabinNodeName();
}
