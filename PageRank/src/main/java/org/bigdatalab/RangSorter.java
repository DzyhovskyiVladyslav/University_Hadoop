package org.bigdatalab;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;

public class RangSorter extends WritableComparator {

    public RangSorter() {
        super(DoubleWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        double thisValue = readDouble(b1, s1);
        double thatValue = readDouble(b2, s2);
        return compare(thisValue, thatValue);
    }

    public static int compare(double d1, double d2) {
        if (d1 > d2)
            return -1;
        if (d1 < d2)
            return 1;
        long thisBits    = Double.doubleToLongBits(d1);
        long anotherBits = Double.doubleToLongBits(d2);
        return (thisBits == anotherBits ?  0 : (thisBits < anotherBits ? 1 : -1));
    }
}

