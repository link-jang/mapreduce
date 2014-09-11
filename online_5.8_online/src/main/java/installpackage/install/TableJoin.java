package installpackage.install;


import java.io.IOException;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TableJoin extends Configured implements Tool{
	
	public static long usrCount = 0;
	public static long allCount = 0;
	public static long joincount = 0;

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length < 1){
    		System.out.println("missing input ");
    		System.exit(1);
    	}
		
		Configuration conf = new Configuration();
		conf.addResource("mapred-site.xml");
    	Job job = new Job(new JobConf(conf));
    	job.setJarByClass(TableJoin.class);
    	job.setMapperClass(JoinMapper.class);
    	job.setReducerClass(JoinReduce.class);
    	job.setJobName("installtaotaosou");
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(IntWritable.class);
    	
    	job.setOutputKeyClass(LongWritable.class);
    	job.setOutputValueClass(LongWritable.class);
    	
    	
    		
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileInputFormat.addInputPath(job, new Path(args[1]));	
    	
    	
    	
    	FileOutputFormat.setOutputPath(job, new Path(args[2]));
    	job.setNumReduceTasks(1);

		return job.waitForCompletion(true)?0:1;
	
	}
	
	
	
	public static class JoinMapper extends  Mapper<LongWritable, Text ,Text,IntWritable>{
		
		private String fileLeft = "0906";
		private String fileRight = "0905";

		@Override
		protected void setup(Context context
                ) throws IOException, InterruptedException {
			
			
			
		}
		
		@Override
		public void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {

			String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
			
			
			String[] line = value.toString().split("\\t");
			if(line.length<3){
				return;
			}
			String peerid = line[0];
			
			context.write(new Text(peerid), new IntWritable(pathName.contains(fileRight)?0:1));
			
		}
    	
    	
    }
	
	public static class JoinReduce extends  Reducer<Text, IntWritable, LongWritable, LongWritable>{
    	
		private int val = -1;
    	@Override
		public void reduce(Text key, Iterable<IntWritable> values,  
                Context context)  {
			// TODO Auto-generated method stub
			int[] tag ={0,0};
			
			for(IntWritable value :values){
				val = value.get();
				switch(val){
				case 0:
					tag[0]+=1;
					break;
				
				case 1:
					tag[1]+=1;
					break;
				default:
					joincount++;
				}
				
			}
			
			if(tag[0]>0){
				usrCount++;
			}
			if(tag[1]>0){
				allCount += tag[0];
			}
    		
//    		if(tag[0]>0 && tag[1]>0){
//    			usrCount++;
//    			allCount += tag[0];
//    			
//    		}
   		

			
		}
    	
    	@Override
    	protected void cleanup(Context context
                ) throws IOException, InterruptedException {
    		context.write(new LongWritable(usrCount),new LongWritable(joincount));
    		context.write(new LongWritable(usrCount),new LongWritable(allCount));
    	}
    }
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new TableJoin(), args); 
    	System.out.println("out result : " + res);
        System.exit(res); 
	}



}
