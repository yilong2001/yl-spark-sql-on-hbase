package com.example.spark.demo.impl.cmp;

import java.io.Serializable;

/**
 * Created by yilong on 2018/6/18.
 */
public class ValueAndCondition implements ValueCondition, Serializable {
    private ValueCondition[] valueConditions;
    public ValueAndCondition(ValueCondition[] valueConditions) {
        this.valueConditions = valueConditions;
    }


    @Override
    public boolean isMatched(Object element) {
        for (ValueCondition vc : valueConditions) {
            if (!vc.isMatched(element)) {
                return false;
            }
        }

        return true;
    }
}
