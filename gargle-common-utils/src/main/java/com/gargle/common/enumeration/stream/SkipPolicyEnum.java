package com.gargle.common.enumeration.stream;

/**
 * ClassName:SkipPolicyEnum
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/05 16:10
 */
public enum SkipPolicyEnum {

    /**
     * 所有配置跳过才跳过
     */
    ALL,

    /**
     * 以内部为准.
     */
    INTERIOR,

    /**
     * 以外部为准.
     */
    EXTERIOR,
}
