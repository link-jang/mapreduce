package installpackage.install;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class InstallApp extends Configured implements Tool {
	
	public static long usrCount = 0;
	public static long allCount = 0;
	public static Map<String,ArrayList<Long>> args = new HashMap<String,ArrayList<Long>>();
	private static String patternFilename = ".*thunderspeed_yxbj.*";
	public static class InstallMapper extends  Mapper<LongWritable, Text ,Text,IntWritable>{
		@Override
		public void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {

			// TODO Auto-generated method stub
			String[] line = value.toString().split("\u0001");
			if(line.length<6){
				return;
			}
			
			String id = line[0];
			String peerid = line[2];
			String filename = line[5];
			if((id.equals("2")||id.equals("724")) && Pattern.matches(patternFilename, filename)){
				context.write(new Text(id + "\t" + peerid), new IntWritable(1));
			}
			
			
			
		}
    	
    	
    	
    	
    }
    
    public static class InstalleReduce extends  Reducer<Text, IntWritable, Text, Text>{
    	
    	@Override
		public void reduce(Text key, Iterable<IntWritable> values,  
                Context context)  {
			// TODO Auto-generated method stub
			
    		if(!key.toString().equals("")){
    			String[] keyToken = key.toString().split("\t");
    			if(keyToken.length != 2)
    			{
    				return;
    			}else{
    				
    				if(args.containsKey(keyToken[0])){
    					usrCount++;
    					List<Long> list  = args.get(keyToken[0]);
    					list.set(0, list.get(0)+1); 
    					
    					long countall = 0;
    					while(values.iterator().hasNext()){
    						countall+=values.iterator().next().get();
    					}
    					
    					list.set(1, list.get(1)+countall); 
    					//args.put(keyToken[0], (ArrayList<Long>) list);
    					
    				}else{
    					
    					List<Long> list = new ArrayList<Long>();
    					list.add(0,1L);
    					
    					
    					long countall = 0;
    					while(values.iterator().hasNext()){
    						countall+=values.iterator().next().get();
    					}
    					list.add(1,countall);
    					args.put(keyToken[0], (ArrayList<Long>) list);
    					
    				}
    				
    			
    	    			
    	    		
    			}
    		
    		}
    		
    		
    		
    		//arg2.collect(new LongWritable(usrCount),new LongWritable(allCount) );
			
		}
    	
    	@Override
    	protected void cleanup(Context context
                ) throws IOException, InterruptedException {
    		for (Entry<String, ArrayList<Long>>  entry :args.entrySet()){
    			context.write(new Text(entry.getKey()),new Text(entry.getValue().get(0) + " \t " + entry.getValue().get(1) + "\t" +entry.getValue().size()));
    			
    		}
    		
    		context.write(new Text("0"),new Text(String.valueOf(usrCount) ));
    		
    	}
    }

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		if(args.length < 1){
    		System.out.println("missing input ");
    		System.exit(1);
    	}
		
		Configuration conf = new Configuration();
    	Job job = new Job(new JobConf(conf));
    	job.setJarByClass(InstallApp.class);
    	job.setMapperClass(InstallMapper.class);
    	job.setReducerClass(InstalleReduce.class);
    	job.setJobName("installtaotaosou");
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(IntWritable.class);
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	
    	FileSystem fs = FileSystem.get(URI.create(args[0]), conf); 
    	FileStatus fileList[] = fs.listStatus(new Path(args[0]));
    	
    	int length = fileList.length;
    	
    	for(int i = 0; i < length; i++){ 
    		
    		FileInputFormat.addInputPath(job, fileList[i].getPath());
    		
    	}
    		
    	
    	
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	job.setNumReduceTasks(1);
    	job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
//		int res = ToolRunner.run(new Configuration(), new InstallApp(), args); 
//    	System.out.println("out result : " + res);
//        System.exit(res); 

		
	}


}
