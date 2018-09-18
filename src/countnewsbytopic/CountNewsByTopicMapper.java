package countnewsbytopic;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class CountNewsByTopicMapper extends Mapper<LongWritable, CountNewsByTopicDBInput, Text, IntWritable>{

	
	@Override
	public void map(LongWritable key, CountNewsByTopicDBInput value, Context context) throws IOException, InterruptedException {
		
		
		String topic = value.getTopic();
		int id = value.getId();
					
		context.write(new Text(topic), new IntWritable(id));
	}
}

