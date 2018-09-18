package toptenheadlinesbysentiment;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;


public class TopTenHeadlinesbySentimentReducer extends Reducer<DoubleWritable, TextArrayWritable, TopTenHeadlinesbySentimentDBOutput, NullWritable>{

	private TreeMap<Double, Pair<Writable, Writable>> repToRecordMap = new TreeMap<Double, Pair<Writable, Writable>>();
	
	@Override
	public void reduce(DoubleWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
		
		for (ArrayWritable value : values) {
			repToRecordMap.put(Double.valueOf(key.get()), new Pair<Writable, Writable>(value.get()[0], value.get()[1]));
			if (repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Double sentiment : repToRecordMap.descendingKeySet()) {
			Pair<Writable, Writable> pair = repToRecordMap.get(sentiment);
			context.write(new TopTenHeadlinesbySentimentDBOutput(sentiment, pair.getFirst().toString(), pair.getSecond().toString()), NullWritable.get());
		}
	}
	
	
}
