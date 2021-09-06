package com.udf;

import com.common.HexBitSet;
import com.util.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFWaActiveDays extends UDF {

    public Long evaluate(String input,Long from ,Long to){
        if (StringUtils.isBlank(input)||input.length()<20){
            return null;
        }
        try {
            return Long.valueOf(HexBitSet.calTrueCount(input,- to.intValue(),-from.intValue()));

        }catch (Exception e ){
            throw new RuntimeException("parameter(input = "+input+" from = "+from+" to = "+ to+")",e);
        }
    }

    public Long evaluate(String input ,Long to){
        if (StringUtils.isBlank(input)||input.length()<20){
            return null;
        }
        return Long.valueOf(HexBitSet.calTrueCount(input,0,to.intValue()));
    }

    public static void main(String[] args) {
        UDFWaActiveDays ud = new UDFWaActiveDays();
        String act_days = "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000800003";
        System.out.println(ud.evaluate(act_days,-6L,0L));
        System.out.println(ud.evaluate(act_days,6L));

    }


}
