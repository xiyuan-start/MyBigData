package FristMapReduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
*该类的功能以及特点描述：这是MapReduce的Reducer类
*主要任务是汇总结果


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


//一个reducer可以接受多个Mapper发送的数据
public class WordReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	
	//记录词频
	//类似于java Integer
	private IntWritable result =new IntWritable();
	
	
	/*key map发送的text
	 * values Map输出的Value的集合     context.write(word,one)   //所有的 one
	 * 
	 *
	*/
	
	public void reduce(Text key,Iterable<IntWritable>values,Context context) throws IOException, InterruptedException
	{
		int sum=0;
		for (IntWritable val:values)
		{
			sum+=val.get();//即get（one）
			
		}
		result.set(sum);//记录一次词频
		context.write(key, result);//单词 +词频
		
	}

	
	
	
	
	
	
	
	
}
