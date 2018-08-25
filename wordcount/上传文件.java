package 文件HDFS;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class 上传文件 {

	public static void main(String[] args) throws IOException, URISyntaxException {
		// TODO 自动生成的方法存根
	    //获取hadoop配置
		Configuration configuration =new Configuration();
		//HDFS连接IP以及端口
		URI url=new URI("hdfs://192.168.10.100:9000");
		
		//获取分布式文件系统
		FileSystem fs= FileSystem.get(url,configuration);
		
		
		//本地文件
		Path src=new Path("C:/Users/luo/Desktop/test.txt");
		
		//HDFS存放位置
		
		Path dst=new Path("/tmp/input/");
		
		//进行文件上传
		fs.copyFromLocalFile(src, dst);
		
		//打印上传信息
		System.out.println("上传到"+configuration.get("fs.defaultFS"));
		
		
		//查看目标目录下的文件信息
		FileStatus files[]=fs.listStatus(dst);
		
		for(FileStatus file:files)
		{
			System.out.println(file.getPath());
		}
		
		
		
	}

}
