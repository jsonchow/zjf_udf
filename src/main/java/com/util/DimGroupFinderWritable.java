package com.util;


import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

/**
 * 优先 有效字段搜索，然后按照时间搜索
 *
 *
 */


public class DimGroupFinderWritable implements Writable {
    private String[] maxValues = new String[0];
    private String[] minValues = new String[0];
    private long max = 0;
    private long min =0;

    //int(100/i)
    private final static int[] WEIGHTS = new int[]{100,50,33,25,20,16,14,12,11,10,9,8,7,7,6,6,5,5,5,5};

    private static int compareNonBlanks(String[] src,String[] target){
        if(src.length>target.length){
            return 1;
        }

        if(target.length>src.length){
            return -1;
        }
        int size = src.length;
        int srcrate = 0;
        int targetrate = 0;
        for (int i = 0; i < size; i++) {
            int weight = i<WEIGHTS.length?WEIGHTS[i]:WEIGHTS[WEIGHTS.length-1];
            if(StringUtils.isNotBlank(src[i])){
                srcrate+=weight;
            }
            if(StringUtils.isNotBlank(target[i])){
                targetrate+=weight;
            }
        }

        return srcrate-targetrate;

    }

    public boolean isEmpty(){return minValues.length==0||maxValues.length==0;}

    public void put(String[] originDims ,long originTm,String[] lstDims,long lstTm){
        if(originDims==null||originDims.length==0||lstDims==null||lstDims.length==0){
            return;
        }

        if(isEmpty()){
            minValues=originDims;
            maxValues=lstDims;
            min=originTm;
            max=lstTm;
            return;
        }

        int cmin = compareNonBlanks(originDims,minValues);
        if(cmin>0||(cmin==0 && originTm<min)){
            minValues=originDims;
            min=originTm;
        }
        int cmax = compareNonBlanks(lstDims,maxValues);
        if(cmax>0||(cmax==0 && lstTm>max)){
            maxValues=originDims;
            max=originTm;
        }

    }

    public static String[] fromArrayWritable(ArrayWritable input){
        Writable[] writable = input.get();
        String[] ret = new String[writable.length];
        for (int i = 0; i < writable.length; i++) {
            ret[i]= Optional.ofNullable(writable[i]).map(Writable::toString).orElseGet(String::new);
        }
        return ret;
    }

    public ArrayWritable terminateArray(){
        if(isEmpty()){
            return null;
        }
        int size = minValues.length;
        Text[] texts = new Text[size*2 +2];
        texts[0] = new Text(min+"");
        texts[size+1] = new Text(max+"");
        for (int i = 0; i < size; i++) {
            texts[i+1]=new Text(minValues[i]);
            texts[size+2+i] = new Text(maxValues[i]);
        }
        return new ArrayWritable(Text.class,texts);
    }

    public ArrayWritable terminateArrayList(){
        if(isEmpty()){
            return null;
        }
        int size = minValues.length;
        ArrayWritable[] arrays = new ArrayWritable[2];

        Text[] minTexts =new Text[size+1];
        minTexts[0] = new Text(min+"");
        for (int i = 0; i < size; i++) {
            minTexts[i+1]=new Text(minValues[i]);
        }
        arrays[0]=new ArrayWritable(Text.class,minTexts);

        Text[] maxTexts = new Text[size+1];
        maxTexts[0] = new Text(max+"");
        for (int i = 0; i < size; i++) {
            maxTexts[i+1]=new Text(maxValues[i]);
        }
        arrays[1]=new ArrayWritable(Text.class,maxTexts);

        return new ArrayWritable(ArrayWritable.class,arrays);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(min);
        WritableUtils.writeStringArray(dataOutput,minValues);
        dataOutput.writeLong(max);
        WritableUtils.writeStringArray(dataOutput,maxValues);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.min=dataInput.readLong();
        this.minValues=WritableUtils.readStringArray(dataInput);
        this.max=dataInput.readLong();
        this.maxValues=WritableUtils.readStringArray(dataInput);
    }
    @Override
    public String toString(){
        return "MinMaxDimGroupWritable{maxValues="+ Arrays.deepToString(maxValues)+"" +
                ", minValues="+Arrays.deepToString(minValues)+",max="+max+",min="+min+"";
    }

    public void merge(DimGroupFinderWritable part){
        if(part==null || part.isEmpty()){
            return;
        }

        if(isEmpty()){
            max = part.max;
            maxValues=part.maxValues;
            min = part.min;
            minValues=part.minValues;
            return;
        }

        int cmin = compareNonBlanks(part.minValues,minValues);
        if(cmin>0||(cmin==0 && part.min<min)){
            minValues= part.minValues;
            min=part.min;
        }
        int cmax = compareNonBlanks(part.maxValues,maxValues);
        if(cmax>0||(cmax==0 && part.max>max)){
            maxValues=part.maxValues;
            max=part.max;
        }


    }

}

