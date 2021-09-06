package com.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * UDAF 实际处理类
 */
public class UDAFMmvgEvaluator extends GenericUDAFEvaluator {

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        return null;
    }

    @Override
    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {

    }

    @Override
    public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {

    }

    @Override
    public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
        return null;
    }

    @Override
    public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {

    }

    @Override
    public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
        return null;
    }
}
