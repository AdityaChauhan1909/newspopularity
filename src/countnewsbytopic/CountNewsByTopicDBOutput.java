package countnewsbytopic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class CountNewsByTopicDBOutput implements Writable, DBWritable{

	private String topic;
	private int count;
	
	public CountNewsByTopicDBOutput(String topic, int count) {
		
		this.topic = topic;
		this.count = count;
	}
	
	public void readFields(DataInput in) throws IOException{
		
	}
	
	public void readFields(ResultSet rs) throws SQLException{
		topic = rs.getString(1);
		count = rs.getInt(2);
	}
	
	public void write(DataOutput out) throws IOException{
		
	}
	
	public void write(PreparedStatement ps) throws SQLException{
		
		ps.setString(1, topic);
		ps.setInt(2, count);
	}
	

	
}
