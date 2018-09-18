package toptenpopularitygoogleplus;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.io.NullWritable;

public class TopTenPopularityGooglePlusDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), 
				new TopTenPopularityGooglePlusDriver(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {
//		

		Configuration conf = new Configuration();
		DBConfiguration.configureDB(conf, 
				"com.mysql.jdbc.Driver", 
				"jdbc:mysql://localhost:3306/NewsPopularity",
				"root",
				"XXXXXXX");

		Job job = new Job(conf);
		job.setJarByClass(TopTenPopularityGooglePlusDriver.class);
		job.setMapperClass(TopTenPopularityGooglePlusMapper.class);
		job.setReducerClass(TopTenPopularityGooglePlusReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TextArrayWritable.class);
		
		job.setOutputKeyClass(TopTenPopularityGooglePlusDBOutput.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(DBInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);
		
		DBInputFormat.setInput(
				job,
				TopTenPopularityGooglePlusDBInput.class,
				"newsdataset",
				null,
				null,
				new String[] {"googleplus", "title", "headline", "sentimentheadline"}
				);
		
		DBOutputFormat.setOutput(job, 
				"toptenheadlinesgoogleplus",
				new String[] {"googleplus", "title", "headline", "sentimentheadline"}
				);
				
		System.exit((job.waitForCompletion(true) ? 0 : 1));
		
		return 1;
	}
	
	
}
	
