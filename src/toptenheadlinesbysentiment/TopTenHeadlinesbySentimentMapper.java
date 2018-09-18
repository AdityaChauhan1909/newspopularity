package toptenheadlinesbysentiment;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;



public class TopTenHeadlinesbySentimentMapper extends Mapper<Object, TopTenHeadlinesbySentimentDBInput, DoubleWritable, TextArrayWritable> {
	
	private TreeMap<Double, Pair<String, String>> repToRecordMap = new TreeMap<Double, Pair<String, String>>();
	
	@Override
	public void map(Object key, TopTenHeadlinesbySentimentDBInput value, Context context) throws IOException, InterruptedException {
		
		
		String title = value.getTitle();
		String headline = value.getHeadline();
		Double sentiment = value.getSentimentHeadline();
		
		Double sent = sentiment;
		repToRecordMap.put(sent, new Pair<String, String>(title, headline));
		
		if (repToRecordMap.size() > 10) {
			repToRecordMap.remove(repToRecordMap.firstKey());
		
		}
	}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Double sent : repToRecordMap.keySet()) {
				Pair<String, String> pair = repToRecordMap.get(sent);
				context.write(new DoubleWritable(sent), TextArrayWritable.from(pair));
			}
	}

}
