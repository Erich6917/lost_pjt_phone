package kv.base;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yqz on 2019/5/9.
 */
public abstract class BaseDimension implements WritableComparable<BaseDimension> {
    public abstract int compareTo(BaseDimension o);
    public abstract void write(DataOutput out) throws IOException;
    public abstract void readFields(DataInput in) throws IOException;
}
