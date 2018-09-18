package toptenpopularitylinkedin;


import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;



public class TopTenPopularityLinkedinMapper extends Mapper<Object, TopTenPopularityLinkedinDBInput, IntWritable, TextArrayWritable> {
	
	private TreeMap<Integer, Pair<String, String, String>> repToRecordMap = new TreeMap<Integer, Pair<String, String, String>>();
	
	@Override
	public void map(Object key, TopTenPopularityLinkedinDBInput value, Context context) throws IOException, InterruptedException {
		
		
		String title = value.getTitle();
		String headline = value.getHeadline();
		Integer linkedin = value.getLinkedin();
		String sentimentheadline = String.valueOf(value.getSentimentHeadline());
		
		Integer link = linkedin;
		repToRecordMap.put(link, new Pair<String, String, String>(title, headline, sentimentheadline));
		
		if (repToRecordMap.size() > 10) {
			repToRecordMap.remove(repToRecordMap.firstKey());
		
		}
	}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Integer link : repToRecordMap.keySet()) {
				Pair<String, String, String> pair = repToRecordMap.get(link);
				context.write(new IntWritable(link), TextArrayWritable.from(pair));
			}
	}

}
