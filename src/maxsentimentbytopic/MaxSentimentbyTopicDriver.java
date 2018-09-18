package maxsentimentbytopic;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;

/*This class is responsible for running map reduce job*/

public class MaxSentimentbyTopicDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception	{
		Configuration conf = new Configuration();
		DBConfiguration.configureDB(conf, 
				"com.mysql.jdbc.Driver", 
				"jdbc:mysql://localhost:3306/NewsPopularity",
				"root",
				"XXXXXXX");
		
		Job job = new Job(conf);
		job.setJarByClass(MaxSentimentbyTopicDriver.class);
		
		job.setMapperClass(MaxSentimentbyTopicMapper.class);
		job.setReducerClass(MaxSentimentbyTopicReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(MaxSentimentbyTopicDBOutput.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(DBInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);
		
		
		DBInputFormat.setInput(
				job,
				MaxSentimentbyTopicDBInput.class,
				"newsdataset",
				null,
				null,
				new String[] {"topic", "sentimenttitle"}
				);
		
		DBOutputFormat.setOutput(job, 
				"maxsentimentbytopic",
				new String[] {"topic", "sentimenttitle"}
				);
				
		System.exit((job.waitForCompletion(true) ? 0 : 1));
		
		return 1;
	}

	public static void main(String[] args) throws Exception {
		MaxSentimentbyTopicDriver driver = new MaxSentimentbyTopicDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}