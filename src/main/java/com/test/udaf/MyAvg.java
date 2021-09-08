package com.test.udaf;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * 计算平均值
 *
 * @Author Daniel
 * @Description New UDAF
 **/

public class MyAvg extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(MyAvg.class.getName());

    /**
     * 参数检测
     * @param parameters
     * @return
     * @throws SemanticException
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {

        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            // 参数类型是原始数据类型
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName() + " is passed.");
        }
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case TIMESTAMP:
                return new AvgEvaluator();
                //如果是以上标准参数类型,直接调用Evaluator类处理
            case BOOLEAN:
            default:
                //如果没有时间参数，则默认报错
                throw new UDFArgumentTypeException(0,
                        "Only numeric or string type arguments are accepted but "
                                + parameters[0].getTypeName() + " is passed.");
        }
    }


    /**
     * Evaluator 重载方法比较多；处理流程跟MapReduce处理过程类似
     * 函数开发之前，要清楚Map、Combiner、Reduce过程的处理逻辑
     * 只有清楚每个阶段的处理逻辑，再能定义清楚每一个阶段的输入、输出对象
     * 针对Avg算平均值类型；
     */
    public static class AvgEvaluator extends GenericUDAFEvaluator {

        //定义全局输入输出数据的类型OI实例，用于解析输入输出数据
        // input For PARTIAL1 and COMPLETE
        PrimitiveObjectInspector inputOI;

        // input For PARTIAL2 and FINAL
        // output For PARTIAL1 and PARTIAL2
        StructObjectInspector soi;
        StructField countField;
        StructField sumField;
        LongObjectInspector countFieldOI;
        DoubleObjectInspector sumFieldOI;

        //定义全局输出数据的类型，用于存储实际数据
        // output For PARTIAL1 and PARTIAL2
        Object[] partialResult;

        // output For FINAL and COMPLETE
        DoubleWritable result;

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            super.init(mode, parameters);

            // init input
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                //部分数据作为输入参数时，用到的struct的OI实例，指定输入数据类型，用于解析数据
                soi = (StructObjectInspector) parameters[0];
                countField = soi.getStructFieldRef("count");
                sumField = soi.getStructFieldRef("sum");
                //数组中的每个数据，需要其各自的基本类型OI实例解析
                countFieldOI = (LongObjectInspector) countField.getFieldObjectInspector();
                sumFieldOI = (DoubleObjectInspector) sumField.getFieldObjectInspector();
            }

            // init output
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {

                //部分聚合结果是一个数组
                partialResult = new Object[2];
                partialResult[0] = new LongWritable(0);
                partialResult[1] = new DoubleWritable(0);
                //构造Struct的OI实例，用于设定聚合结果数组的类型,需要字段名List和字段类型List作为参数来构造
                ArrayList<String> fname = new ArrayList<String>();
                fname.add("count");
                fname.add("sum");
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                //此处的两个OI类型 描述的是 partialResult[] 的两个类型，所以在这里是一样的
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
            } else {
                //FINAL 最终聚合结果为一个数值(hadoop中的数据类型)，并用基本类型OI设定其类型
                result = new DoubleWritable(0);
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            }
        }

        //聚合数据缓存存储结构
        static class AverageAgg implements AggregationBuffer {
            long count;
            double sum;
        }

        //获得新的聚合对象
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AverageAgg result = new AverageAgg();
            reset(result);
            return result;
        }

        //重置聚合对象
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            AverageAgg myagg = (AverageAgg) agg;
            myagg.count = 0;
            myagg.sum = 0;
        }

        boolean warned = false;

        //遍历原始数据
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];
            if (p != null) {
                AverageAgg myagg = (AverageAgg) agg;
                try {
                    //通过基本数据类型OI解析Object p的值
                    double v = PrimitiveObjectInspectorUtils.getDouble(p, inputOI);
                    myagg.count++;
                    myagg.sum += v;
                } catch (NumberFormatException e) {
                    if (!warned) {
                        warned = true;
                        LOG.warn(getClass().getSimpleName() + " "
                                + StringUtils.stringifyException(e));
                        LOG.warn(getClass().getSimpleName()
                                + " ignoring similar exceptions.");
                    }
                }
            }
        }

        //得出部分聚合结果
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            AverageAgg myagg = (AverageAgg) agg;
            ((LongWritable) partialResult[0]).set(myagg.count);
            ((DoubleWritable) partialResult[1]).set(myagg.sum);
            return partialResult;
        }

        //合并部分聚合结果
        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial != null) {
                AverageAgg myagg = (AverageAgg) agg;
                //通过StandardStructObjectInspector实例，分解出 partial 数组元素值
                Object partialCount = soi.getStructFieldData(partial, countField);
                Object partialSum = soi.getStructFieldData(partial, sumField);
                //通过基本数据类型的OI实例解析Object的值
                myagg.count += countFieldOI.get(partialCount);
                myagg.sum += sumFieldOI.get(partialSum);
            }
        }

        //最终聚合结果
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            AverageAgg myagg = (AverageAgg) agg;
            if (myagg.count == 0) {
                return null;
            } else {
                result.set(myagg.sum / myagg.count);
                return result;
            }
        }
    }



}

