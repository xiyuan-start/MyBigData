package FristMapReduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



/*
*该类的功能以及特点描述：这是MapReduce的Mapper类
*主要任务是将任务分配的过程


*该类是否被编译测试：否
*@see(与之相关联的类)：   来源：
*                     中间：
*                     去处：
*开发公司或单位：成007个人
*版权：成007

*@author(作者)：成007
*@since（该文件使用的jdk）：JDK1.8
*@version（版本）：1.0
*@数据库查询方式：
*@date(开发日期)：2018/5/24
*改进：
*最后更改日期：
*/

//继承Mapper抽象类


//将数据源转换成Mapper可以处理的类型
//并且准备把中间结果分配给Reducer
public class WordMapper extends Mapper<Object,Text,Text,IntWritable>{
	
	
	public final static IntWritable one=new IntWritable(1);
	
	private Text word =new Text();
	
	//Mapper抽象类的核心方法，三个参数
	//key 首字符偏移量
	//value文件的一行类容           *****利用map开始切割成无数的<key,value>......
	//context Map端的上下文  
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException
	{
		
		//StringTokenizer是一个用来分隔String的应用类
		//java默认的分隔符是“空格”、“制表符(‘\t’)”、“换行符(‘\n’)”、“回车符(‘\r’)”
		StringTokenizer itr=new StringTokenizer(value.toString());
		
		//hasMoreTokens()判断是否还有分隔符
		//
		while(itr.hasMoreTokens())
		{
			
			//itr.nextToken()以指点的分割符 获取的结果  
			//这里就是一个单词
			word.set(itr.nextToken());
			//一个单词发送一次
			context.write(word,one);
			
		}
		
	}
	
	
	
	
	

}
