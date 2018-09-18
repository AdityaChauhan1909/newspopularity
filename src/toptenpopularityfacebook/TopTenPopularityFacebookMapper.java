package toptenpopularityfacebook;


import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;



public class TopTenPopularityFacebookMapper extends Mapper<Object, TopTenPopularityFacebookDBInput, IntWritable, TextArrayWritable> {
	
	private TreeMap<Integer, Pair<String, String, String>> repToRecordMap = new TreeMap<Integer, Pair<String, String, String>>();
	
	@Override
	public void map(Object key, TopTenPopularityFacebookDBInput value, Context context) throws IOException, InterruptedException {
		
		
		String title = value.getTitle();
		String headline = value.getHeadline();
		Integer facebook = value.getFacebook();
		String sentimentheadline = String.valueOf(value.getSentimentHeadline());
		
		Integer fb = facebook;
		repToRecordMap.put(fb, new Pair<String, String, String>(title, headline, sentimentheadline));
		
		if (repToRecordMap.size() > 10) {
			repToRecordMap.remove(repToRecordMap.firstKey());
		
		}
	}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Integer fb : repToRecordMap.keySet()) {
				Pair<String, String, String> pair = repToRecordMap.get(fb);
				context.write(new IntWritable(fb), TextArrayWritable.from(pair));
			}
	}

}
