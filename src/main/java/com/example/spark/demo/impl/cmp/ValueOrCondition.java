package com.example.spark.demo.impl.cmp;

import java.io.Serializable;

/**
 * Created by yilong on 2018/6/18.
 */
public class ValueOrCondition implements ValueCondition, Serializable {
    private ValueCondition[] valueConditions;
    public ValueOrCondition(ValueCondition[] valueConditions) {
        this.valueConditions = valueConditions;
    }

    @Override
    public boolean isMatched(Object element) {
        for (ValueCondition vc : valueConditions) {
            if (vc.isMatched(element)) {
                return true;
            }
        }

        return false;
    }
}
