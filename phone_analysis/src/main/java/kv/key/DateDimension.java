package kv.key;


import kv.base.BaseDimension;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class DateDimension extends BaseDimension{
    private String year;
    private String month;
    private String day;

    @Override
    public int compareTo(BaseDimension o) {
        DateDimension o1 = (DateDimension) o;
        int result = this.year.compareTo(o1.year);
        if (result != 0) {
            return result;
        }

        result = this.month.compareTo(o1.month);
        if (result != 0) {
            return result;
        }

        result = this.day.compareTo(o1.day);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.year);
        out.writeUTF(this.month);
        out.writeUTF(this.day);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readUTF();
        this.month = in.readUTF();
        this.day = in.readUTF();
    }
}
