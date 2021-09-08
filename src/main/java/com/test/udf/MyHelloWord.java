package com.test.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author zhoujianfeng
 */
public class MyHelloWord extends UDF {

    public String evaluate(String word){
        return "say ["+word + "]";
    }
}
