package toptenpopularitygoogleplus;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;



public class TopTenPopularityGooglePlusMapper extends Mapper<Object, TopTenPopularityGooglePlusDBInput, IntWritable, TextArrayWritable> {
	
	private TreeMap<Integer, Pair<String, String, String>> repToRecordMap = new TreeMap<Integer, Pair<String, String, String>>();
	
	@Override
	public void map(Object key, TopTenPopularityGooglePlusDBInput value, Context context) throws IOException, InterruptedException {
		
		
		String title = value.getTitle();
		String headline = value.getHeadline();
		Integer googleplus= value.getGoogleplus();
		String sentimentheadline = String.valueOf(value.getSentimentHeadline());
		
		Integer google = googleplus;
		repToRecordMap.put(google, new Pair<String, String, String>(title, headline, sentimentheadline));
		
		if (repToRecordMap.size() > 10) {
			repToRecordMap.remove(repToRecordMap.firstKey());
		
		}
	}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Integer google : repToRecordMap.keySet()) {
				Pair<String, String, String> pair = repToRecordMap.get(google);
				context.write(new IntWritable(google), TextArrayWritable.from(pair));
			}
	}

}
