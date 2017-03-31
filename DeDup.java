import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class DeDup extends Configured implements Tool{
	static int field;
	
	
public static class DeDupMap extends MapReduceBase implements Mapper<LongWritable, Text,
Text, IntWritable>{

public void map(LongWritable ikey, Text ival, OutputCollector<Text, IntWritable> output, Reporter reporter)
throws IOException{
	
	String value=ival.toString();
	System.out.println("value: " + value);
	
	String [] row=value.split("\\s+");
	System.out.println("Size: " + row.length + " row[0]: " + Integer.parseInt(row[0]));
	
	IntWritable id=new IntWritable(Integer.parseInt(row[0]));
	Text word=new Text(row[field]);	
	
	output.collect(word, id);	
}
}

public static class DeDupReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text,Text>{

	public void reduce(Text ikey, Iterator< IntWritable> vlist, OutputCollector<Text, Text> output, Reporter reporter)
throws IOException{

		System.out.println("ikey:  " + ikey);
		
		Set<Integer> st= new LinkedHashSet<Integer>();
		
		String s="";
		while(vlist.hasNext()) {
			
			IntWritable in=(IntWritable) vlist.next();
			st.add(in.get());
			
			
		}
		
		
		s="";
		for(Integer i: st) {
			s +=  " " + i;
			System.out.println(" s: " + s);
			
		}
		
		
		
		output.collect(ikey, new Text(s));	
	
}
	
}


public static void main(String[] args) throws Exception{
int res = ToolRunner.run(new Configuration(), new DeDup(), args);
System.exit(res);
}

public int run(String[] args) throws Exception{
pass1();
pass2();
pass3();
pass4();
pass5();
return 0;
}


public void pass1() throws Exception{
// Job configuration
	
	DeDup.field=1;
	
JobConf conf= new JobConf(DeDup.class);
conf.setJobName("Map-Reduce example");
conf.setOutputKeyClass(Text.class);
conf.setOutputValueClass(IntWritable.class);
conf.setMapperClass(DeDupMap.class);
conf.setReducerClass(DeDupReduce.class);

conf.setInputFormat(TextInputFormat.class);
conf.setOutputFormat(TextOutputFormat.class);
// HDFS input and output directory

FileInputFormat.addInputPath(conf, new Path("input"));
FileOutputFormat.setOutputPath(conf, new Path("name"));

JobClient.runJob(conf);
}

public void pass2() throws Exception{
	// Job configuration/home/hduser/workspace/DeDup/field1
		
		DeDup.field=2;
		
	JobConf conf= new JobConf(DeDup.class);
	conf.setJobName("Map-Reduce example");
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);
	conf.setMapperClass(DeDupMap.class);
	conf.setReducerClass(DeDupReduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
	// HDFS input and output directory

	FileInputFormat.addInputPath(conf, new Path("input"));
	FileOutputFormat.setOutputPath(conf, new Path("class"));

	JobClient.runJob(conf);
	}
public void pass3() throws Exception{
	// Job configuration
		
		DeDup.field=3;
		
	JobConf conf= new JobConf(DeDup.class);
	conf.setJobName("Map-Reduce example");
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);
	conf.setMapperClass(DeDupMap.class);
	conf.setReducerClass(DeDupReduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
	// HDFS input and output directory

	FileInputFormat.addInputPath(conf, new Path("input"));
	FileOutputFormat.setOutputPath(conf, new Path("city"));

	JobClient.runJob(conf);
	}

public void pass4() throws Exception{
	// Job configuration
		
		DeDup.field=4;
		
	JobConf conf= new JobConf(DeDup.class);
	conf.setJobName("Map-Reduce example");
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);
	conf.setMapperClass(DeDupMap.class);
	conf.setReducerClass(DeDupReduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
	// HDFS input and output directory

	FileInputFormat.addInputPath(conf, new Path("input"));
	FileOutputFormat.setOutputPath(conf, new Path("branch"));

	JobClient.runJob(conf);
	}

public void pass5() throws Exception{
	// Job configuration
		
		DeDup.field=5;
		
	JobConf conf= new JobConf(DeDup.class);
	conf.setJobName("Map-Reduce example");
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);
	conf.setMapperClass(DeDupMap.class);
	conf.setReducerClass(DeDupReduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
	// HDFS input and output directory

	FileInputFormat.addInputPath(conf, new Path("input"));
	FileOutputFormat.setOutputPath(conf, new Path("rollno"));

	JobClient.runJob(conf);
	}


	}
