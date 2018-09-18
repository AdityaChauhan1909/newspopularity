package meansentimentbytopic;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MeanSentimentbyTopicMapper extends Mapper<LongWritable, MeanSentimentbyTopicDBInput, Text, DoubleWritable> {

		
	@Override
	public void map(LongWritable key, MeanSentimentbyTopicDBInput value, Context context) throws IOException, InterruptedException {
		
		String topic = value.getTopic();
		Double sentimentTitle = value.getSentimentTitle();
		
		context.write(new Text(topic), new DoubleWritable(sentimentTitle));
	}
}