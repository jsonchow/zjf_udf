package com.udaf;


import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;

/**
 * 寻找最早/最晚时间且有效的数据组
 *
 * 参数1 为时间戳
 * 参数2 为字段数字组
 * 参数3 N，用于判断数据组，前N项 数组不为空 为有效数据组
 * 返回值： 【【 最早有效时间戳，最早有效数组】，【最晚有效时间戳，最晚有效数据组】】，数组长度：2N*2
 */

public class UDAFMinMaxValidGroup extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        return super.getEvaluator(info);
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        return super.getEvaluator(info);
    }



}
