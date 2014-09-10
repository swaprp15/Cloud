package cloud.mywordcount;

/*
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
*/

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;





public class MyWordCountMain {

	// args as input file and output directory
	public static void main(String[] args) throws Exception
	{
		//Job job = new Job();
		JobConf job = new JobConf();
		
		job.setJarByClass(MyWordCountMain.class);
		job.setJobName("My Word Count");
		
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		//FileInputFormat.addInputPath(job, new Path(args[1]));
		//FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setMapperClass(MyWordCountMapper.class);
		job.setReducerClass(MyWordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		JobClient.runJob(job);
		
	}
	
}
