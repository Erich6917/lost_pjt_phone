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
@NoArgsConstructor
@AllArgsConstructor
public class ContactDimension extends BaseDimension{
    //联系人维度
    private String telephone;
    private String name;

    @Override
    public int compareTo(BaseDimension o) {
        ContactDimension o1 = (ContactDimension) o;
        int result = this.name.compareTo(o1.name);
        if (result != 0) {
            return result;
        }
        result = this.telephone.compareTo(o1.telephone);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.telephone);
        out.writeUTF(this.name);
    }

    //ctrl + enter
    @Override
    public void readFields(DataInput in) throws IOException {
        this.telephone = in.readUTF();
        this.name = in.readUTF();
    }
}
