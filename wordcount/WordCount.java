package ²âÊÔ¶þ;


	import java.io.IOException;
	import java.net.URI;
	import java.util.StringTokenizer;

	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.FileSystem;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

	public class WordCount {
	  static final String IN_PUT = "hdfs://192.168.10.100:9000/tmp/input";  
	  static final String OUT_PUT = "hdfs://192.168.10.100:9000/tmp/output";
	  
	  public static class TokenizerMapper extends Mapper {
	    
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	      
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens()) {
	      word.set(itr.nextToken());
	      context.write(word, one);
	      }
	    }
	  }
	  
	  public static class IntSumReducer extends Reducer {
	    private IntWritable result = new IntWritable();

	public void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values){
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }
	  }

	  
	  
	  
	  public static void main(String[] args0) throws Exception {
	    Configuration conf = new Configuration();
	FileSystem fileSystem = FileSystem.get(new URI(IN_PUT), conf);     
	    Path outPath = new Path(OUT_PUT);  
		conf.set("fs.defaultFS","hdfs://192.168.10.100:9000");
		conf.set("mapred.job.tracker", "hdfs://192.168.10.100:9001");  
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.jobhistory.address", "192.168.10.100:10020");
		//conf.set("hadoop.job.user","root");
		conf.set("yarn.resourcemanager.address","192.168.10.100:8032");
        conf.set("yarn.resourcemanager.admin.address", "192.168.10.100:8033");
		conf.set("yarn.resourcemanager.scheduler.address","192.168.10.100:8030");
		conf.set("yarn.resourcemanager.resource-tracker.address", "192.168.10.100:8031");
	    if(fileSystem.exists(outPath)){  
	        fileSystem.delete(outPath,true);  
	    }  
	    Job job = new Job(conf, "word count");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(IN_PUT));
	    FileOutputFormat.setOutputPath(job,outPath);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}
