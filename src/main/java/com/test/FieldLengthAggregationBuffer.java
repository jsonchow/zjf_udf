package com.test;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.util.JavaDataModel;

public class FieldLengthAggregationBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {

    private Integer value=0;

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    /**
     *异步操作，buffer对象的value值一直累加
     * @param addValue
     */
    public void add(int addValue){
        synchronized (value){
            value += addValue;
        }
    }

    /**
     * 合并值缓冲区大小；这里用于保存字符长度大小，因此用4byte
     * @return
     */
    @Override
    public int estimate() {
        return JavaDataModel.PRIMITIVES1;
    }



}
