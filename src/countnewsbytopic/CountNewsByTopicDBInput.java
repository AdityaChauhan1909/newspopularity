package countnewsbytopic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class CountNewsByTopicDBInput implements Writable, DBWritable{

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
		id = rs.getInt(2);
		topic = rs.getString(1);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	
	@Override
	public void write(PreparedStatement ps) throws SQLException {
		ps.setInt(2, id);
		ps.setString(1, topic);

	
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


