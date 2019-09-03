package kv.key;

import kv.base.BaseDimension;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ComDimension extends BaseDimension {
    private ContactDimension contactDimension = new ContactDimension();
    private DateDimension dateDimension = new DateDimension();

    @Override
    public int compareTo(BaseDimension o) {
        ComDimension o1 = (ComDimension) o;
        int result = this.dateDimension.compareTo(o1.dateDimension);
        if (result != 0) {
            return result;
        }
        result = this.contactDimension.compareTo(o1.contactDimension);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        contactDimension.write(out);
        dateDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        contactDimension.readFields(in);
        dateDimension.readFields(in);
    }
}
