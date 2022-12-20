package com.gargle.common.stream.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Documented
@Retention(RUNTIME)
@Target({METHOD, TYPE})
public @interface Locomotive {
    /**
     * 当前执行器节点名称
     */
    String locomotiveNodeName();

    /**
     * 当前执行器节点对应的步骤名称
     */
    String step() default "default";

    /**
     * 当前执行器节点步骤的优先级, 越小越优先执行,
     * 相同node和step不允许有相同的 order,
     * 相同node和step 存在多个时,只加载order最小的那个作为执行链执行步骤.
     */
    double order() default 100;
}
