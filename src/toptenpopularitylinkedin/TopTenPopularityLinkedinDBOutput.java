package toptenpopularitylinkedin;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class TopTenPopularityLinkedinDBOutput implements Writable, DBWritable{
	
	private String title;
	private String headline;
	private Integer linkedin;
	private String sentimentheadline;
	
	public TopTenPopularityLinkedinDBOutput(Integer linkedin,String title, String headline, String sentimentheadline) {
		this.linkedin = linkedin;
		this.title = title;
		this.headline = headline;
		this.sentimentheadline = sentimentheadline;
		
		
	}
	
	public void readFields(DataInput in) throws IOException{
		
	}
	
	public void readFields(ResultSet rs) throws SQLException{
		linkedin = rs.getInt(1);
		title = rs.getString(2);
		headline = rs.getString(3);
		sentimentheadline = rs.getString(4);
		
	}
	
	public void write(DataOutput out) throws IOException{
		
	}
	
	public void write(PreparedStatement ps) throws SQLException{
		ps.setInt(1, linkedin);
		ps.setString(2, title);
		ps.setString(3, headline);
		ps.setString(4,sentimentheadline);
		
	}
		
}


