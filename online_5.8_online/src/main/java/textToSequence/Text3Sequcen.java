package textToSequence;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Text3Sequcen {
	
	public static void main(String[] args ) throws IOException{
		
		String inputUrl = args[0];
		String outputUrl = args[1];
		
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(URI.create(inputUrl),conf);
		FileStatus[] status_list = fs.listStatus(new Path(inputUrl));  
		for (FileStatus status : status_list) { 
			
			org.apache.hadoop.mapred.FileSplit split = (org.apache.hadoop.mapred.FileSplit)getFullSplit(status);  
	        org.apache.hadoop.mapred.LineRecordReader reader =  
	                new org.apache.hadoop.mapred.LineRecordReader(conf, split);  
	        Text textValue = new Text();  
	        LongWritable pos = new LongWritable();  
	        while (reader.next(pos, textValue)) {  
	            String line = new String(textValue.getBytes(), 0, textValue.getLength(), "UTF-8");  
	            result.add(line);
	        }
		
		
	}
	

}
