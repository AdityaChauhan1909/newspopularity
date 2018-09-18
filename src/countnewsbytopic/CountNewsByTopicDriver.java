package countnewsbytopic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public class CountNewsByTopicDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception	{
		Configuration conf = new Configuration();
		DBConfiguration.configureDB(conf, 
				"com.mysql.jdbc.Driver", 
				"jdbc:mysql://localhost:3306/NewsPopularity",
				"root",
				"XXXXXXXX");
		
		Job job = new Job(conf);
		job.setJarByClass(CountNewsByTopicDriver.class);
		
		job.setMapperClass(CountNewsByTopicMapper.class);
		job.setReducerClass(CountNewsByTopicReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(CountNewsByTopicDBOutput.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(DBInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);
		
		
		DBInputFormat.setInput(
				job,
				CountNewsByTopicDBInput.class,
				"newsdataset",
				null,
				null,
				new String[] {"topic", "id"}
				);
		
		DBOutputFormat.setOutput(job, 
				"countnewsbytopic",
				new String[] {"topic", "count"}
				);
				
		System.exit((job.waitForCompletion(true) ? 0 : 1));
		
		return 1;
	}

	public static void main(String[] args) throws Exception {
		CountNewsByTopicDriver driver = new CountNewsByTopicDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
	
	
	
}
