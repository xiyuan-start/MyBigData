package FristMapReduce;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class WordMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO 自动生成的方法存根

		//读取Hadoop配置
		Configuration conf=new Configuration();
		//在window 建议用set自己进行配置
		conf.set("fs.defaultFS","hdfs://192.168.10.100:9000");
		//conf.set("mapred.job.tracker", "hdfs://192.168.10.100:9001");  
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.jobhistory.address", "192.168.10.100:10020");
		//conf.set("hadoop.job.user","root");
		conf.set("yarn.resourcemanager.address","192.168.10.100:8032");
        conf.set("yarn.resourcemanager.admin.address", "192.168.10.100:8033");
		conf.set("yarn.resourcemanager.scheduler.address","192.168.10.100:8030");
		conf.set("yarn.resourcemanager.resource-tracker.address", "192.168.10.100:8031");
		//将命令行中的参数自动设置到变量conf中
		String []otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
		
		//输入路径和输出路径没有报错
		
		if(otherArgs.length!=2)
		{
			System.err.println("缺少输入或者输出文件路径");
			System.exit(2);
		}	
		
		
		Job job=new Job(conf,"word count");//新建一个job,传入配置信息
		
		job.setJarByClass(WordMain.class);//设置主类
		job.setMapperClass(WordMapper.class);//设置Mapper类
		job.setReducerClass(WordReducer.class);//设置Reducer类
		job.setCombinerClass(WordReducer.class);//设置作业合成类
		
		job.setOutputKeyClass(Text.class);//设置输出数据的关键类
		job.setOutputValueClass(IntWritable.class);//设置输出值类
		
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//文件输入
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//文件输出

		System.exit(job.waitForCompletion(true)? 0:1);//等待完成退出
		
		
		
		
	}

}
