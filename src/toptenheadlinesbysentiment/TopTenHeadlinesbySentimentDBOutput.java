package toptenheadlinesbysentiment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class TopTenHeadlinesbySentimentDBOutput implements Writable, DBWritable{
	
	private String title;
	private String headline;
	private Double sentiment;
	
	public TopTenHeadlinesbySentimentDBOutput(Double sentiment,String title, String headline) {
		this.sentiment = sentiment;
		this.title = title;
		this.headline = headline;
		
		
	}
	
	public void readFields(DataInput in) throws IOException{
		
	}
	
	public void readFields(ResultSet rs) throws SQLException{
		sentiment = rs.getDouble(1);
		title = rs.getString(2);
		headline = rs.getString(3);
		
	}
	
	public void write(DataOutput out) throws IOException{
		
	}
	
	public void write(PreparedStatement ps) throws SQLException{
		ps.setDouble(1, sentiment);
		ps.setString(2, title);
		ps.setString(3, headline);
		
	}
		
}


