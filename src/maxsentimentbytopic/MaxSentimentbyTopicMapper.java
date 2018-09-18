package maxsentimentbytopic;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxSentimentbyTopicMapper extends Mapper<LongWritable, MaxSentimentbyTopicDBInput, Text, DoubleWritable> {

	
	@Override
	public void map(LongWritable key, MaxSentimentbyTopicDBInput value, Context context) throws IOException, InterruptedException {
		
		
		String topic = value.getTopic();
		Double sentimentTitle = value.getSentimentTitle();
					
		context.write(new Text(topic), new DoubleWritable(sentimentTitle));
	}
}