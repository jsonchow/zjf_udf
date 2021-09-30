package com.util;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public class ActiveBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {

    private int[] activeArray;
    private int limit =0;

    public ActiveBuffer(int limit) {
        this.limit = limit;
        this.activeArray=new int[0];
    }



}
