package meansentimentbytopic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class MeanSentimentbyTopicDBInput implements Writable, DBWritable {

	private int id;
	private String title;
	private String headline;
	private String source;
	private String topic;
	private Double sentimenttitle;
	private Double sentimentheadline;
	private int facebook;
	private int googleplus;
	private int linkedin;
	
	public void readFields(DataInput in) throws IOException {
		
	}
	
	public void readFields(ResultSet rs) throws SQLException {
		//id = rs.getInt(1);
		//title = rs.getString(2);
		//headline = rs.getString(3);
		//source = rs.getString(4);
		topic = rs.getString(1);
		sentimenttitle = rs.getDouble(2);
		//sentimentheadline = rs.getDouble(7);
		//facebook = rs.getInt(8);
		//googleplus = rs.getInt(9);
		//linkedin = rs.getInt(10);
					
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	
	@Override
	public void write(PreparedStatement ps) throws SQLException {
//		ps.setInt(1, id);
//		ps.setString(2, title);
//		ps.setString(3, headline);
//		ps.setString(4, source);
		ps.setString(1, topic);
		ps.setDouble(2, sentimenttitle);
//		ps.setDouble(7,sentimentheadline);
//		ps.setInt(8, facebook);
//		ps.setInt(9,googleplus);
//		ps.setInt(10, linkedin);
//		
	}
	
	public int getId() {
		return id;
	}
	
	public String getTitle() {
		return title;
	}

	public String getHeadline() {
		return headline;
	}
	
	public String getSource() {
		return source;
	}
	
	public String getTopic() {
		return topic;
	}
	
	public Double getSentimentTitle() {
		return sentimenttitle;
	}
	
	public Double getSentimentHeadline() {
		return sentimentheadline;
	}
	
	public int getFacebook() {
		return facebook;
	}
	
	public int getGoogleplus() {
		return googleplus;
	}
	
	public int getLinkedin() {
		return linkedin;
	}
	
}


