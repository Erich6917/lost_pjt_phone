package kv.value;

import kv.base.BaseValue;
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
public class CountDurationValue extends BaseValue {
    private String callSum;
    private String callDurationSum;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.callSum);
        dataOutput.writeUTF(this.callDurationSum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.callSum = dataInput.readUTF();
        this.callDurationSum = dataInput.readUTF();
    }
}
