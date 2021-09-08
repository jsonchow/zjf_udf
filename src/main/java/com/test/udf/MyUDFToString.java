package com.test.udf;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.io.*;

/**
 * @author zhoujianfeng
 */
public class MyUDFToString extends UDF {

    private final Text t = new Text();
    private final ByteStream.Output out = new ByteStream.Output();
    private final byte[] trueBytes = new byte[]{84, 82, 85, 69};
    private final byte[] falseBytes = new byte[]{70, 65, 76, 83, 69};

    public MyUDFToString() {
    }

    public Text evaluate(NullWritable i) {
        return null;
    }

    public Text evaluate(BooleanWritable i) {
        if (i == null) {
            return null;
        } else {
            this.t.clear();
            this.t.set(i.get() ? this.trueBytes : this.falseBytes);
            return this.t;
        }
    }

    public Text evaluate(ByteWritable i) {
        if (i == null) {
            return null;
        } else {
            this.out.reset();
            LazyInteger.writeUTF8NoException(this.out, i.get());
            this.t.set(this.out.getData(), 0, this.out.getLength());
            return this.t;
        }
    }

    public Text evaluate(ShortWritable i) {
        if (i == null) {
            return null;
        } else {
            this.out.reset();
            LazyInteger.writeUTF8NoException(this.out, i.get());
            this.t.set(this.out.getData(), 0, this.out.getLength());
            return this.t;
        }
    }

    public Text evaluate(IntWritable i) {
        if (i == null) {
            return null;
        } else {
            this.out.reset();
            LazyInteger.writeUTF8NoException(this.out, i.get());
            this.t.set(this.out.getData(), 0, this.out.getLength());
            return this.t;
        }
    }

    public Text evaluate(LongWritable i) {
        if (i == null) {
            return null;
        } else {
            this.out.reset();
            LazyLong.writeUTF8NoException(this.out, i.get());
            this.t.set(this.out.getData(), 0, this.out.getLength());
            return this.t;
        }
    }

    public Text evaluate(FloatWritable i) {
        if (i == null) {
            return null;
        } else {
            this.t.set(i.toString());
            return this.t;
        }
    }

    public Text evaluate(DoubleWritable i) {
        if (i == null) {
            return null;
        } else {
            this.t.set(i.toString());
            return this.t;
        }
    }

    public Text evaluate(Text i) {
        if (i == null) {
            return null;
        } else {
            i.set(i.toString());
            return i;
        }
    }

    public Text evaluate(DateWritable d) {
        if (d == null) {
            return null;
        } else {
            this.t.set(d.toString());
            return this.t;
        }
    }

    public Text evaluate(TimestampWritable i) {
        if (i == null) {
            return null;
        } else {
            this.t.set(i.toString());
            return this.t;
        }
    }

    public Text evaluate(HiveDecimalWritable i) {
        if (i == null) {
            return null;
        } else {
            this.t.set(i.toString());
            return this.t;
        }
    }

    public Text evaluate(BytesWritable bw) {
        if (null == bw) {
            return null;
        } else {
            this.t.set(bw.getBytes(), 0, bw.getLength());
            return this.t;
        }
    }
}
