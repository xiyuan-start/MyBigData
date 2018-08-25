package MapReduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import MapReduce.WordCount.InvertedIndexMapper.InvertedIndexCombiner;
import MapReduce.WordCount.InvertedIndexMapper.InvertedIndexReducer;

/*
 *倒排索引：
 *应用：全文搜索引擎
 *原理：根据类容查找文档的方式
 *相比较Wordcount key是单词与文档URI的组合
 *URL是URI的子集  URI是唯一表示符
 *
*/
public class WordCount {
	
	
	//Mapper类
	//1,2是接受类容
	//3.4是转换目标
	public static class InvertedIndexMapper extends Mapper<Object,Text,Text,Text>
	{
		
		private Text keyInfo =new Text();//存储单词和URI的组合
		private Text valueInfo=new Text();//存储词频
		private FileSplit split;   //存储Split对象  用来获得文件的URI
		
		
		//map接受和发送的都是<key,value>结构的数据      
		//key为偏移量
		//value为一行的文本内容
		//context 进行传输
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException
		{
			//获得<key,value>对所属的FileSplit对象
			split=(FileSplit)context.getInputSplit();
			
			//StringTokenizer是一个用来分隔String的应用类
			//java默认的分隔符是“空格”、“制表符(‘\t’)”、“换行符(‘\n’)”、“回车符(‘\r’)”
			StringTokenizer itr=new StringTokenizer(value.toString());
			
			while(itr.hasMoreTokens())
			{
				
				//itr.nextToken()以指点的分割符 获取的结果  
			    //设置key  由单词与URI组成
				keyInfo.set(itr.nextToken()+":"+split.getPath().toString());
				//设置词频，即value
				valueInfo.set("1");
				
				context.write(keyInfo, valueInfo);
	
			}
			
			
		}
		
		
		
		//Combiner类
		//对Mapper收集来的<key,value>进行处理   从新设定key 实现单词为键,  URI与词频组成 value
		//Reduce就是对<key,value>进行处理的过程
		
		//<goog:text1,1> <goog:text1,2>   values 看做所有有相同key   value的集合     
		//先集合 在同一处理
		
		
		public static class InvertedIndexCombiner extends Reducer<Text,Text,Text,Text>
		{
			private Text info=new Text();//新的value
			
			//因为一reduce 会处理很多组数据
			public void reduce(Text key,Iterable<Text>values,Context context) throws IOException, InterruptedException
			{
			//统计词频
			int sum=0;
			for(Text value:values)
			{
				sum+=Integer.parseInt(value.toString());//将字符串转换成Int
				
			}
			
			//:的位置
			int splitIndex=key.toString().indexOf(":");
			
			if(splitIndex!=-1)//防止内存溢出
			{
			
			
			//拆分key  形成新的<key,value>
			
			//sum始终记录的是一个文档中一种单词的数量
			info.set(key.toString().substring(splitIndex+1)+":"+sum);
			
			key.set(key.toString().substring(0,splitIndex));//新的key

			
			context.write(key, info);
			}
			
			}
			
		}
		
		
		
		
		//Reducer
		//对<key,value>作最后的汇总处理
		
		public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>
		{
			private Text result =new Text();
			
			
			//因为一reduce 会处理很多组数据
			public void reduce(Text key,Iterable<Text>values,Context context) throws IOException, InterruptedException
			{

			String list=new String();
			for(Text value:values)
			{
				
				list+=value.toString()+";";
				
				
			}
			result.set(list);
	
			context.write(key, result);
			
			
			}
			
			
		}
		
		
		
	}
	

		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			// TODO 自动生成的方法存根

			//读取Hadoop配置
			Configuration conf=new Configuration();
			//在window 建议用set自己进行配置
			conf.set("fs.defaultFS","hdfs://192.168.137.100:9000");
			//conf.set("mapred.job.tracker", "hdfs://192.168.10.100:9001");  
			conf.set("mapreduce.framework.name", "yarn");
			conf.set("mapreduce.jobhistory.address", "192.168.137.100:10020");
			//conf.set("hadoop.job.user","root");
			conf.set("yarn.resourcemanager.address","192.168.137.100:8032");
	        conf.set("yarn.resourcemanager.admin.address", "192.168.137.100:8033");
			conf.set("yarn.resourcemanager.scheduler.address","192.168.137.100:8030");
			conf.set("yarn.resourcemanager.resource-tracker.address", "192.168.137.100:8031");
			//将命令行中的参数自动设置到变量conf中
			String []otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
			
			//输入路径和输出路径没有报错
			
			if(otherArgs.length!=2)
			{
				System.err.println("缺少输入或者输出文件路径");
				System.exit(2);
			}	
			
			
			Job job=new Job(conf,"word count");//新建一个job,传入配置信息
			
			job.setJarByClass(WordCount.class);//设置主类
			job.setMapperClass(InvertedIndexMapper.class);//设置Mapper类
			job.setReducerClass(InvertedIndexReducer.class);//设置Reducer类
			job.setCombinerClass(InvertedIndexCombiner.class);//设置作业合成类
			
			job.setOutputKeyClass(Text.class);//设置输出数据的关键类
			job.setOutputValueClass(Text.class);//设置输出值类
			
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//文件输入
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//文件输出

			System.exit(job.waitForCompletion(true)? 0:1);//等待完成退出
			
			
			
			
		
		}
	
	
	
	

}
