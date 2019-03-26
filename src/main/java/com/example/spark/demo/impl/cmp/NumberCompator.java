package com.example.spark.demo.impl.cmp;

import com.example.spark.sql.util.ORMUtils;

import java.io.Serializable;

/**
 * Created by yilong on 2018/6/11.
 */
public abstract class NumberCompator implements ValueCompator,Serializable {
    private String field;
    private Object target;
    public NumberCompator(String field, Object target) {
        this.field = field;
        this.target = target;
    }

    @Override
    public int compare(Object element) {
        Object value = ORMUtils.getSqlFieldValue(element, field);
        Class type = ORMUtils.getSqlFieldType(element, field);

        if (type.getCanonicalName().equals("java.lang.String")) {
            return value.toString().compareTo(target.toString());
        }

        if (type.getCanonicalName().equals("java.lang.Long")) {
            Long src = Long.parseLong(value.toString());
            Long tg = Long.parseLong(target.toString());
            return src.compareTo(tg);
        }

        if (type.getCanonicalName().equals("java.lang.Integer")) {
            Integer src = Integer.parseInt(value.toString());
            Integer tg = Integer.parseInt(target.toString());
            return src.compareTo(tg);
        }

        if (type.getCanonicalName().equals("java.lang.Boolean")) {
            Boolean src = Boolean.parseBoolean(value.toString());
            Boolean tg = Boolean.parseBoolean(target.toString());
            return src.compareTo(tg);
        }

        return -1;
    }
}
