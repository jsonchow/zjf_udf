package com.test;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public class FieldLengthUDAFEvaluator extends GenericUDAFEvaluator {

    PrimitiveObjectInspector inputOI;
    ObjectInspector outputOI;
    PrimitiveObjectInspector integerOI;

    /**
     * 每个阶段都会执行的方法
     * 把每个阶段要用到的输入输出inspector 好，其他方法调用的时候可以直接用
     * @param m
     * @param parameters
     * @return
     * @throws HiveException
     */
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        super.init(m, parameters);

        //COPLETE 或者 PARTIAL1，输入的都是数据库原始数据
        if (Mode.PARTIAL1.equals(m) || Mode.COMPLETE.equals(m)) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
        } else {
            //PARIAL2 和 FINAL 阶段，都是基于前一个阶段init返回值作为parameters入参
            integerOI=(PrimitiveObjectInspector) parameters[0];
        }

        outputOI = ObjectInspectorFactory.getReflectionObjectInspector(
                Integer.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA
        );

        return outputOI;

    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        return new FieldLengthAggregationBuffer();
    }

    /**
     * 重置，将总数清理掉
     * @param aggregationBuffer
     * @throws HiveException
     */
    @Override
    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
        ((FieldLengthAggregationBuffer)aggregationBuffer).setValue(0);
    }

    /**
     * 不断被调用执行的方法，最终数据都保存在agg中
     * @param agg
     * @param parameters
     * @throws HiveException
     */
    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
        if(null==parameters || parameters.length<1){
            return;
        }

        Object javaObj = inputOI.getPrimitiveJavaObject(parameters[0]);

        ((FieldLengthAggregationBuffer)agg).add(String.valueOf(javaObj).length());
    }

    /**
     * 当前阶段结束时执行的方法，返回的是部分聚合结果（map、combiner）
     * @param agg
     * @return
     * @throws HiveException
     */
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
        return terminate(agg);
    }

    /**
     * 合并数据，将总长度加入到缓存对象中（combiner和reducer）
     * @param agg
     * @param partial
     * @throws HiveException
     */
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
        ((FieldLengthAggregationBuffer)agg).add((Integer)integerOI.getPrimitiveJavaObject(partial));
    }

    /**
     * group by 的时候返回当前分组的最终结果
     * @param agg
     * @return
     * @throws HiveException
     */
    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
        return ((FieldLengthAggregationBuffer)agg).getValue();
    }
}
