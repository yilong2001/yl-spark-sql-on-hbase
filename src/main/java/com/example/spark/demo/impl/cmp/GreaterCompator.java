package com.example.spark.demo.impl.cmp;

import com.example.spark.sql.util.ORMUtil;

import java.io.Serializable;

/**
 * Created by yilong on 2018/6/11.
 */
public class GreaterCompator extends NumberCompator implements ValueCondition, Serializable {
    public GreaterCompator(String field, String target) {
        super(field, target);
    }


    @Override
    public boolean isMatched(Object element) {
        return compare(element) > 0;
    }
}
