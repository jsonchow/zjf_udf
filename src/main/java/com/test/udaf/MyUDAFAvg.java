package com.test.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

import java.util.ArrayList;

public class MyUDAFAvg extends AbstractGenericUDAFResolver {


    /**
     * 参数检查和调用Evaluator处理器
     * @param info
     * @return
     * @throws SemanticException
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {

        if(info.length != 1){
            throw new UDFArgumentTypeException(info.length ,"只需要一个参数");
        }

        if(info[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException(0,"参数只能为主参数类型"+info[0].getCategory());
        }

        switch (((PrimitiveTypeInfo)info[0]).getPrimitiveCategory()){
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case TIMESTAMP:
                return new MyAvgEvaluator();
            //如果是以上标准参数类型,直接调用Evaluator类处理
            case BOOLEAN:
            default:
                //如果没有时间参数，则默认报错
                throw new UDFArgumentTypeException(0,
                        "Only numeric or string type arguments are accepted but "
                                + info[0].getTypeName() + " is passed.");
        }


    }


    public static class MyAvgEvaluator extends GenericUDAFEvaluator{

        //定义每个阶段的输入输出参数
        //partial阶段的输入
        PrimitiveObjectInspector inputOI;

        //中间结果-输入
        StructObjectInspector soi;
        StructField countF;
        StructField sumF;
        LongObjectInspector countOI;
        DoubleObjectInspector sumOI;

        //中间结果-输出
        Object[] paritalResult;

        //最终结果输出
        DoubleWritable result;


        @AggregationType(estimable = true)
        static class AvgBuffer extends AbstractAggregationBuffer{
            long count;
            double sum;

            @Override
            public int estimate() {
                return super.estimate();
            }
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length==1);
            super.init(m, parameters);

            //input
            if(m==Mode.PARTIAL1 || m==Mode.COMPLETE){ //map阶段输入;COMPLETE状态只有map没有reduce
                inputOI = (PrimitiveObjectInspector) parameters[0];
            }else{
                soi = (StructObjectInspector)parameters[0];
                countF = soi.getStructFieldRef("count");
                sumF = soi.getStructFieldRef("sum");

                countOI = (LongObjectInspector) countF.getFieldObjectInspector();
                sumOI = (DoubleObjectInspector) sumF.getFieldObjectInspector();
            }

            //ouput
            if(m==Mode.PARTIAL2 || m==Mode.PARTIAL1){
                paritalResult = new Object[2];
                paritalResult[0] = new LongWritable(0);
                paritalResult[1] = new DoubleWritable(0);

                ArrayList<String> f_name = new ArrayList<String>();
                f_name.add("count");
                f_name.add("sum");

                ArrayList<ObjectInspector> f_obj = new ArrayList<ObjectInspector>();
                f_obj.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                f_obj.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

                return ObjectInspectorFactory.getStandardStructObjectInspector(f_name,f_obj);

            }else{
                //最终输出结果
                result = new DoubleWritable(0);
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            }


        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AvgBuffer agg = new AvgBuffer();
            reset(agg);
            return agg;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((AvgBuffer) agg).count=0;
            ((AvgBuffer) agg).sum=0;

        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length==1);
            Object p = parameters[0];
            if(p!=null){
                AvgBuffer ab = (AvgBuffer) agg;

                PrimitiveObjectInspectorUtils.getDouble(p,inputOI);

            }


        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            AvgBuffer ab = (AvgBuffer) agg;
            ((LongWritable)paritalResult[0]).set(ab.count);
            ((DoubleWritable)paritalResult[1]).set(ab.sum);
            return paritalResult;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {

        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            AvgBuffer ab = (AvgBuffer) agg;
            if(ab.count==0){
                return null;
            }else{
                result.set(ab.sum/ab.count);
                return result;
            }

        }
    }

}
