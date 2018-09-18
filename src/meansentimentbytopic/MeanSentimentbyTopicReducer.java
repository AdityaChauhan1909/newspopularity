package meansentimentbytopic;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MeanSentimentbyTopicReducer extends Reducer<Text, DoubleWritable, MeanSentimentbyTopicDBOutput, NullWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		Double sum = 0.0;
		int count = 0;
		Double mean = 0.0;

		for (DoubleWritable value : values) {
			sum += value.get();
			++count;
		}

		mean = sum / count;
		
		context.write(new MeanSentimentbyTopicDBOutput(key.toString(),mean), NullWritable.get());
	}
}