package com.gargle.common.stream.annotation;

import com.gargle.common.stream.context.base.BaseEntity;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Documented
@Retention(RUNTIME)
@Target({METHOD, TYPE})
public @interface Cabin {

    /**
     * 当前执行器节点名称
     */
    String cabinNodeName();

    /**
     * 当前执行器节点对应的步骤名称
     */
    String step();

    /**
     * 当前执行器节点步骤的优先级, 越小越优先执行,
     * 相同node和step不允许有相同的 order,
     * 相同node和step 存在多个时,只加载order最小的那个作为执行链执行步骤.
     */
    double order() default 100;

    /**
     * 是否跳过当前步骤.
     */
    boolean skip() default false;

    /**
     * 若node: service 有 A,B,C 三步. 且 A,B 的  parallelism为 100 , C 的 parallelism为 101
     * 则 在node: service中, 并行执行 A,B.  然后再执行C.
     */
    int parallelism() default 100;

    /**
     * 当前节点将信息转换为实体类的类模板class.
     */
    Class<? extends BaseEntity> entityClass() default BaseEntity.class;
}