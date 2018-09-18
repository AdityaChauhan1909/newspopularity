package countnewsbytopic;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class CountNewsByTopicReducer extends Reducer<Text, IntWritable, CountNewsByTopicDBOutput, NullWritable> {
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int count = 0;

		for (IntWritable value : values) {
			count++;
					
		}

		context.write(new CountNewsByTopicDBOutput(key.toString(),count), NullWritable.get());
	}

	
	
	
}
