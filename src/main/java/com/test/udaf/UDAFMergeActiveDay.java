package com.test.udaf;

import com.test.NewUDAF;
import com.util.ActiveBuffer;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


public class UDAFMergeActiveDay extends AbstractGenericUDAFResolver {


    /**
     * 参数重载操作，判断是否合法
     * @param info
     * 原参数说明：
     *  偏移当前天数n，偏移前n天行为量，向前偏移m天，n+m天前历史日活字段，日活天数限制->当天日活字段
     *  n=0,代表当天，为null忽略该值
     *  m=null 代表不偏移，可选。1表示在n基础上在偏移 m天
     *  0，null，pv，365
     *
     * @return
     * @throws SemanticException
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {

        //判断参数个数
        if(info.length!=5){
            throw new UDFArgumentLengthException("函数传参个数错误！参数个数必须为5个！" +
                    "第一个参数为日期偏移量；" +
                    "第二个参数为有效日志条数；" +
                    "第三个参数为日期偏移量；" +
                    "第四个参数为lastActDays；" +
                    "第五个参数为act_days格式，不设置默认为120天，一般设置为365天");
        }

        //判断参数类型,参数必须为java基本数据类型
        if(info[0].getCategory()!= ObjectInspector.Category.PRIMITIVE
                ||info[1].getCategory()!= ObjectInspector.Category.PRIMITIVE
                ||info[2].getCategory()!= ObjectInspector.Category.PRIMITIVE
                ||info[3].getCategory()!= ObjectInspector.Category.PRIMITIVE
                ||info[4].getCategory()!= ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException();

        }

        PrimitiveObjectInspector.PrimitiveCategory p1 = ((PrimitiveTypeInfo)info[0]).getPrimitiveCategory();
        PrimitiveObjectInspector.PrimitiveCategory p2 = ((PrimitiveTypeInfo)info[1]).getPrimitiveCategory();
        PrimitiveObjectInspector.PrimitiveCategory p3 = ((PrimitiveTypeInfo)info[2]).getPrimitiveCategory();
        PrimitiveObjectInspector.PrimitiveCategory p4 = ((PrimitiveTypeInfo)info[3]).getPrimitiveCategory();
        PrimitiveObjectInspector.PrimitiveCategory p5 = ((PrimitiveTypeInfo)info[4]).getPrimitiveCategory();

        //第一个参数类型判断
        switch (p1){
            case SHORT:
            case INT:
            case LONG:
                break;
            case BYTE:
            case STRING:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case TIMESTAMP:
            default:
                throw new UDFArgumentTypeException();
        }
        //第二个参数类型判断
        switch (p2){
            case SHORT:
            case INT:
            case LONG:
                break;
            case BYTE:
            case STRING:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case TIMESTAMP:
            default:
                throw new UDFArgumentTypeException();
        }
        //第三个参数类型判断
        switch (p3){
            case SHORT:
            case INT:
            case LONG:
                break;
            case BYTE:
            case STRING:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case TIMESTAMP:
            default:
                throw new UDFArgumentTypeException();
        }
        //第四个参数类型判断
        switch (p4){
            case VOID:
            case STRING:
                break;
            case INT:
            case LONG:
            case BYTE:
            case SHORT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case TIMESTAMP:
            default:
                throw new UDFArgumentTypeException();
        }
        //第五个参数类型判断
        switch (p5){
            case SHORT:
            case INT:
                break;
            case BYTE:
            case LONG:
            case STRING:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case TIMESTAMP:
            default:
                throw new UDFArgumentTypeException();
        }
        //执行UDAF函数处理器
        return new MergeActiveEvaluator();
    }




//        ActiveArrayWritable writable = (ActiveArrayWritable)buffer;
//        int offset = (int)((LongWritable)args[0]).get();
//        long pv = args[1]==null ? 0:((LongWritable)args[1]).get();
//        int offset1 = args[2]==null ? 0:(int)((LongWritable)args[2]).get();
//        String lastActDays = args[3]==null ? null:((Text)args[3]).toString();
//        int limit = args[4]==null ? 120:(int)((LongWritable)args[4]).get();
//        writable.add(offset,pv,offset1,lastActDays,limit);




    public static class MergeActiveEvaluator extends GenericUDAFEvaluator{

        /**
         * 参数定义
         */
        PrimitiveObjectInspector p1LongOI; //参数1
        PrimitiveObjectInspector p2LongOI; //参数2
        PrimitiveObjectInspector p3LongOI; //参数3
        PrimitiveObjectInspector p4StringOI; //参数4
        PrimitiveObjectInspector p5IntOI; //参数5

        /**
         * MR各阶段计算输入输出参数初始化
         * @param m
         * mode阶段主要分为 Partial1、Partial2、Final、Complete 四个阶段
         * Partial1阶段对应map过程，主要调用iterate 和 terminatePartial方法
         * Partial2阶段对应combiner过程，调用merge 和 terminatePartial 方法
         * Final阶段对应Reduce过程，调用merge 和 terminate 方法
         * Complete阶段对应只有map没有reduce过程，调用iterate 和 terminate方法
         * @param parameters
         * @return
         * @throws HiveException
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            //输入参数初始化
            if(m == Mode.PARTIAL1 || m == Mode.COMPLETE){
                p1LongOI = (PrimitiveObjectInspector) parameters[0];
                p2LongOI = (PrimitiveObjectInspector) parameters[1];
                p3LongOI = (PrimitiveObjectInspector) parameters[2];
                p4StringOI = (PrimitiveObjectInspector) parameters[3];
                p5IntOI = (PrimitiveObjectInspector) parameters[4];

            }else{
                //中间buffer解析

            }


            //输出参数初始化




            return super.init(m, parameters);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            ActiveBuffer ab = new ActiveBuffer(0);
            reset(ab);
            return ab;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ActiveBuffer ab = (ActiveBuffer)agg;

        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {

        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return null;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {

        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            return null;
        }
    }
}
