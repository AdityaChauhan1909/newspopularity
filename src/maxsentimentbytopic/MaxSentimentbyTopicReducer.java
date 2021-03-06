package maxsentimentbytopic;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxSentimentbyTopicReducer extends Reducer<Text, DoubleWritable, MaxSentimentbyTopicDBOutput, NullWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		Double maxValue = Double.MIN_VALUE;

		for (DoubleWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}

		context.write(new MaxSentimentbyTopicDBOutput(key.toString(), maxValue), NullWritable.get());
	}
}