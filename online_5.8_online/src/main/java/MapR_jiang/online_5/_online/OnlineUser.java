package MapR_jiang.online_5._online;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





/**
 * Hello world!
 *
 */
public class OnlineUser extends Configured implements Tool 
{
	
	public static long usrCount = 0;
	public static long allCount = 0;
	
    public static void main( String[] args ) throws Exception
    {
        
    	int res = ToolRunner.run(new Configuration(), new OnlineUser(), args); 
    	System.out.println(usrCount + " " + allCount);
    	System.out.println("out result : " + res);
        System.exit(res); 
    	
    	
    }
    
    private static String pattern58 = "^5.8.*$";
    
    public static class OnlineMapper extends  Mapper<Text, Text ,Text,IntWritable>{

		public void map(Text key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String[] line = value.toString().split("\u0001");
			if(line.length<4){
				return;
			}
			
			String peerid = line[0];
			String thunderVersion = line[3];
			
			if(!Pattern.matches(pattern58, thunderVersion)){
				return;
			}
			
			output.collect(new Text(peerid), new IntWritable(1));
		}
    	
    	
    	
    	
    }
    
    public static class OnlineReduce extends  Reducer<Text, IntWritable, LongWritable, LongWritable>{

		public void reduce(Text key, Iterator<IntWritable> value,
				OutputCollector<LongWritable, LongWritable> arg2, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			
    		if(!key.toString().equals("")){
    			usrCount += 1;
    		}
    		
    		while(value.hasNext()){
    			allCount += value.next().get();
    		}
    		
    		
    		//arg2.collect(new LongWritable(usrCount),new LongWritable(allCount) );
			
		}
    }

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		if(args.length < 2){
    		System.out.println("missing input ");
    		System.exit(1);
    	}
    
    	Job job = new Job();
    	job.setJarByClass(OnlineUser.class);
    	job.setMapperClass(MapR_jiang.online_5._online.OnlineUser.OnlineMapper.class);
    	job.setReducerClass(MapR_jiang.online_5._online.OnlineUser.OnlineReduce.class);
    	job.setJobName("online5.8");

    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	job.setNumReduceTasks(1);
    	job.waitForCompletion(true);
		return 0;
	}
}
