package mr.design.patterns.meta.mergedjob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaggedText implements WritableComparable<TaggedText> {

  private String tag = "";
  private final Text text = new Text();

  public TaggedText() {}

  public TaggedText(TaggedText text) {
    setTag(text.getTag());
    setText(text.getText());
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public String getTag() {
    return tag;
  }

  public void setText(Text text) {
    this.text.set(text);
  }

  public void setText(String text) {
    this.text.set(text);
  }

  public Text getText() {
    return text;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    tag = in.readUTF();
    text.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(tag);
    text.write(out);
  }

  @Override
  public int compareTo(TaggedText obj) {
    int compare = tag.compareTo(obj.getTag());
    if (compare == 0) {
      return text.compareTo(obj.getText());
    } else {
      return compare;
    }
  }

  @Override
  public String toString() {
    return tag + ":" + text.toString();
  }
}