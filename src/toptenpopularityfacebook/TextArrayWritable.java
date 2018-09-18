package toptenpopularityfacebook;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TextArrayWritable extends ArrayWritable{

	public TextArrayWritable() {
		super(Text.class);
	}

	public TextArrayWritable(Pair<Writable, Writable, Writable> pair) {
		super(Text.class, new Text[] { (Text) pair.getFirst(),
				(Text) pair.getSecond(), 
				(Text)pair.getThird()});
	}

	public static TextArrayWritable from(Pair<String, String, String> pair) {
		Pair<Writable, Writable, Writable> p = new Pair<Writable, Writable, Writable>(new Text(
				pair.getFirst()), new Text(pair.getSecond()), new Text(pair.getThird()));
		return new TextArrayWritable(p);
	}

	@Override
	public String toString() {
		Writable[] writables = this.get();
		if (writables == null || writables.length == 0) {
			return "";
		}
		StringBuilder sb = new StringBuilder(10 * writables.length);
		for (Writable w : this.get()) {
			sb.append(w.toString());
			sb.append('\t');
		}
		return sb.toString().trim();
	}
	
}
