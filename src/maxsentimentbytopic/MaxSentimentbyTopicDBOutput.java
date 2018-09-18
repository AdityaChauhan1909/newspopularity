package maxsentimentbytopic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class MaxSentimentbyTopicDBOutput implements Writable, DBWritable {

	private String topic;
	private Double maxSent;
	
	public MaxSentimentbyTopicDBOutput(String topic, Double maxSent) {
		
		this.topic = topic;
		this.maxSent = maxSent;
	}
	
	public void readFields(DataInput in) throws IOException{
		
	}
	
	public void readFields(ResultSet rs) throws SQLException{
		topic = rs.getString(1);
		maxSent = rs.getDouble(2);
	}
	
	public void write(DataOutput out) throws IOException{
		
	}
	
	public void write(PreparedStatement ps) throws SQLException{
		
		ps.setString(1, topic);
		ps.setDouble(2, maxSent);
	}
	
}
