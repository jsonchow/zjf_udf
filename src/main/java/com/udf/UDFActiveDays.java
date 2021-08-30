package com.udf;

import com.common.HexBitSet;
import com.util.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFActiveDays extends UDF {

    public Long evaluate(String input,Long from ,Long days){
        if (StringUtils.isBlank(input)){
            return 0L;
        }
        try {
            return Long.valueOf(HexBitSet.calTrueCount(input,from.intValue(),from.intValue()+days.intValue()-1));

        }catch (Exception e ){
            throw new RuntimeException("input = "+input+" from = "+from+" days = "+ days,e);
        }
    }

    public Long evaluate(String input ,Long days){
        return evaluate(input,0L,days);
    }
    public Long evaluate(String input){
        return evaluate(input,0L,1L);
    }

    public static void main(String[] args) {
        UDFActiveDays ud = new UDFActiveDays();
        System.out.println("00000000000000000000000000000000000000000000000000000000000000000000000000000000000000800000".length());
        System.out.println(ud.evaluate("00000000000000000000000000000000000000000000000000000000000000000000000000000000000000800000",-6L,0L));
    }

}

