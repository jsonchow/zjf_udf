package com.test.udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hive.service.cli.HiveSQLException;

/**
 * This class implements the COUNT aggregation function as in SQL.
 *
 * 本函数处理特点：map阶段针对parameter进行分块处理，如果是针对distinct 多列语法，
 * 在map分块处理，就会保持每个map分配处理的 pre columns 不一样，以便后面merge直接累加
 *
 *
 */
@Description(name = "count",
        value = "_FUNC_(*) - Returns the total number of retrieved rows, including "
                + "rows containing NULL values.\n"

                + "_FUNC_(expr) - Returns the number of rows for which the supplied "
                + "expression is non-NULL.\n"

                + "_FUNC_(DISTINCT expr[, expr...]) - Returns the number of rows for "
                + "which the supplied expression(s) are unique and non-NULL.")
public class MyUDAFCount implements GenericUDAFResolver2 {


    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {

        return new CountEvaluator();

    }

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {

        ObjectInspector[] parameters = info.getParameterObjectInspectors();

        if (parameters.length == 0) {
            if (!info.isAllColumns()) {
                throw new UDFArgumentException("Argument expected");
            }
            assert !info.isDistinct() : "DISTINCT not supported with *";
        } else {
            if (parameters.length > 1 && !info.isDistinct()) {
                throw new UDFArgumentException("DISTINCT keyword must be specified");
            }
            assert !info.isAllColumns() : "* not supported in expression list";
        }

        CountEvaluator countEvaluator = new CountEvaluator();
        countEvaluator.setCountAllColumns(info.isAllColumns());
        countEvaluator.setCountDistinct(info.isDistinct());

        return countEvaluator;
    }


    public static class CountEvaluator extends GenericUDAFEvaluator {
        private boolean countAllColumns = false;
        private boolean countDistinct = false;

        private LongWritable result; //输出对象
        private ObjectInspector[] inputOI, outputOI; //输入/中间输出（下一阶段输入）
        private LongObjectInspector partialCountAggOI; //partial 阶段的处理器


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
                partialCountAggOI = (LongObjectInspector) parameters[0];
            } else {
                inputOI = parameters;
                outputOI = ObjectInspectorUtils.getStandardObjectInspector(inputOI,
                        ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA); //根据输入参数的类型，拷贝复制他的输出类型
            }
            result = new LongWritable(0);
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        public void setCountAllColumns(boolean countAllColumns) {
            this.countAllColumns = countAllColumns;
        }


        public void setCountDistinct(boolean countDistinct) {
            this.countDistinct = countDistinct;
        }

        @AggregationType(estimable = true)
        static class CountAgg extends AbstractAggregationBuffer {
            Object[] prevColumns = null;
            long value;

            @Override
            public int estimate() {
                return JavaDataModel.PRIMITIVES2;
            }
        }


        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            CountAgg buffer = new CountAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((CountAgg) agg).prevColumns = null;
            ((CountAgg) agg).value = 0;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {

            if (parameters == null) {//无参数不处理
                return;
            }

            if (countAllColumns) { //参数为* 通配符
                assert parameters.length == 0;
                ((CountAgg) agg).value++;
            } else {//不包含通配符 *
                boolean countThisRow = true;
                assert parameters.length !=0:parameters.length;
                for (Object nextPara : parameters) {
                    if (nextPara == null) {
                        countThisRow = false;
                        break;
                    }
                }

                if (countThisRow && countDistinct) {//distinct重复行
                    Object[] preColumns = ((CountAgg) agg).prevColumns;
                    if (preColumns == null) {
                        ((CountAgg) agg).prevColumns = new Object[parameters.length];
                    } else if (ObjectInspectorUtils.compare(parameters, inputOI, preColumns, outputOI) == 0) {
                        //处理行存在后面就不处理这一行
                        countThisRow = false;
                    }
                }

                if (countThisRow) {
                    //处理行不存在就放到 prevColumns 里面; 因为是大数据结算，不同阶段 map、combiner、reduce;都是计算局部排重count
                    ((CountAgg) agg).prevColumns = ObjectInspectorUtils.copyToStandardObject(
                            parameters, inputOI, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA
                    );
                }

                if (countThisRow) { //不包含distinct
                    ((CountAgg) agg).value++;
                }

            }


        }


        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                long p = partialCountAggOI.get(partial);
                ((CountAgg) agg).value += p;
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            result.set(((CountAgg) agg).value);
            return result;
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

    }

}
