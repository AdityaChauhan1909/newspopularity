package toptenpopularitylinkedin;


import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;


public class TopTenPopularityLinkedinReducer extends Reducer<IntWritable, TextArrayWritable, TopTenPopularityLinkedinDBOutput, NullWritable>{

	private TreeMap<Integer, Pair<Writable, Writable, Writable>> repToRecordMap = new TreeMap<Integer, Pair<Writable, Writable, Writable>>();
	
	@Override
	public void reduce(IntWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
		
		for (ArrayWritable value : values) {
			repToRecordMap.put(Integer.valueOf(key.get()), new Pair<Writable, Writable, Writable>(value.get()[0], value.get()[1], value.get()[2]));
			if (repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Integer linkedin : repToRecordMap.descendingKeySet()) {
			Pair<Writable, Writable, Writable> pair = repToRecordMap.get(linkedin);
			context.write(new TopTenPopularityLinkedinDBOutput(linkedin, pair.getFirst().toString(), pair.getSecond().toString(), pair.getThird().toString()), NullWritable.get());
		}
	}
	
	
}
