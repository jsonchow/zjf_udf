package com.util;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class ActiveArrayWritable implements Writable {

    private int[] activeArray;
    private int limit =0;

    public ActiveArrayWritable(int limit) {
        this.limit = limit;
        this.activeArray=new int[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        int size=activeArray.length;
        dataOutput.writeInt(size);
        for (int i = 0; i < size; i++) {
            dataOutput.writeInt(activeArray[i]);
        }
        dataOutput.writeInt(limit);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        this.activeArray = new int[size];
        for (int i = 0; i < size; i++) {
            this.activeArray[i]=dataInput.readInt();
        }
        this.limit = dataInput.readInt();
    }

    private static int[] shiftLeft(int[] mag,int n){
        int nInts = n >>> 5;
        int nBits = n & 0x1f;
        int magLen = mag.length;
        int[] newMag = null;

        if(nBits == 0){
            newMag=new int[magLen+nInts];
            System.arraycopy(mag,0,newMag,0,magLen);
        }else{
            int i=0;
            int nBits2 = 32-nBits;
            int highBits = mag[0] >>> nBits2;
            if(highBits!=0){
                newMag=new int[magLen+nInts+1];
                newMag[i++]=highBits;
            }else{
                newMag=new int[magLen + nInts];
            }

            int j=0;
            while (j<magLen-1){
                newMag[i++]=mag[j++]<< nBits | mag[j]>>>nBits2;
            }
            newMag[i]=mag[j]<<nBits;
        }


        return newMag;
    }

    public static int[] hexString2IntegerArray(String input,int limit){
        int size = (limit+31)>>>5;
        int realLen = size <<3;
        int radix = 16;
        int[] mag = new int[size];
        int begin = Math.max(0,input.length()-realLen);
        int end = input.length();
        char[] dst = new char[realLen];
        Arrays.fill(dst,0,realLen,'0');
        int dstBegin = Math.max(0,realLen-input.length());
        input.getChars(begin,end,dst,dstBegin);
        int len = 8;
        int index =0;
        for (int i = 0; i < size; i++) {
            int digit = 0;
            int j=0;
            int result = 0;
            while (j<len){
                char s = dst[index++];
                digit=s=='0'?0:Character.digit(s,radix);
                result*=radix;
                result+=digit;
                j++;
            }
            mag[i]=result;
        }
        return mag;
    }


    public static String toHexString(int[] mag,int limit){
        char[] sb = new char[mag.length*8];
        int index = 0;
        for (int i = 0; i < mag.length; i++) {
            sb[index++]=(Character.forDigit((mag[i]&0xf0000000)>>>7 *4,16));
            sb[index++]=(Character.forDigit((mag[i]&0x0f000000)>>>6 *4,16));
            sb[index++]=(Character.forDigit((mag[i]&0x00f00000)>>>5 *4,16));
            sb[index++]=(Character.forDigit((mag[i]&0x000f0000)>>>4 *4,16));
            sb[index++]=(Character.forDigit((mag[i]&0x0000f000)>>>3 *4,16));
            sb[index++]=(Character.forDigit((mag[i]&0x00000f00)>>>2 *4,16));
            sb[index++]=(Character.forDigit((mag[i]&0x000000f0)>>>1 *4,16));
            sb[index++]=(Character.forDigit((mag[i]&0x0000000f),16));
        }
        int len = (limit+3)/4;
        int begin = Math.max(sb.length-len,0);
        return new String(sb,begin,len);
    }

    public void init(int limit){
        this.limit=limit;
        int size = (limit +31)>>>5;

        if (this.activeArray==null|this.activeArray.length==0) {
            this.activeArray=new int[size];
            Arrays.fill(this.activeArray,0);
            return;
        }

        if (this.activeArray.length<size) {
            int[] dst = new int[size];

            Arrays.fill(dst,0);
            int begin = Math.max(0,size-this.activeArray.length);
            System.arraycopy(this.activeArray,0,dst,begin,this.activeArray.length);
            this.activeArray=dst;
            return;
        }

        if (this.activeArray.length>size) {
            int[] dst = new int[size];

            Arrays.fill(dst,0);
            int begin = Math.max(0,size-this.activeArray.length);
            System.arraycopy(this.activeArray,0,dst,begin,this.activeArray.length);
            this.activeArray=dst;
            return;
        }


    }

    public void add(int offset,long pv,int offset1,String lastActDays,int limit){
        if (offset<0){
            return;
        }

        this.init(limit);
        if(lastActDays!=null){
            int[] lastActs = shiftLeft(hexString2IntegerArray(lastActDays,limit),offset1+offset);
            int curIndex = this.activeArray.length-1;
            int lastIndex = lastActs.length-1;
            while (true){
                this.activeArray[curIndex]=this.activeArray[curIndex]|lastActs[lastIndex];
                curIndex--;
                lastIndex--;
                if(curIndex<=0 || lastIndex<=0){
                    break;
                }
            }
        }

        if(pv<=0){
            return;
        }
        int size = (limit+31)>>>5;
        int pos=Math.max(0,(size-(offset>>>5)-1));
        int index = offset%32;
        this.activeArray[pos]=this.activeArray[pos]|(1<<index);


    }

    public boolean isEmpty(){return this.limit ==0 || this.activeArray ==null ||this.activeArray.length==0;}

    public void merge(ActiveArrayWritable other){
        if(other.isEmpty()){
            return;
        }

        if(this.isEmpty()&&!other.isEmpty()){
            this.limit = other.limit;
            this.activeArray=Arrays.copyOf(other.activeArray,other.activeArray.length);
            return ;
        }

        int curIndex = this.activeArray.length-1;
        int lastIndex = other.activeArray.length-1;

        while (true){
            this.activeArray[curIndex]=this.activeArray[curIndex]|other.activeArray[lastIndex];
            curIndex--;
            lastIndex--;
            if(curIndex<=0||lastIndex<=0){
                break;
            }
        }


    }

    public Text terminate(){
        if(this.isEmpty()){
            return null;
        }

        return new Text(toHexString(this.activeArray,this.limit));
    }

}
