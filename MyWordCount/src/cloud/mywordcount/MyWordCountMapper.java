package cloud.mywordcount;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MyWordCountMapper implements Mapper<LongWritable, Text, Text, IntWritable> 
{
/*
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		
		String inputLine = value.toString();
		
		String parts[] = inputLine.split(" ");
		
		for (String word : parts) 
		{
			if(map.containsKey(word))
			{
				map.put(word, map.get(word)+1);
			}
			else
				map.put(word, 1);
		}
		
		for (String word : map.keySet()) 
		{
			context.write(new Text(word), new IntWritable(map.get(word)));
		}
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.run(context);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	} */

	private static final IntWritable one = new IntWritable(1);
	
	private Text word = new Text();

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(LongWritable arg0, Text value,
			OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		
		String inputLine = value.toString();
		
		String parts[] = inputLine.split(" ");
		
		for (String word : parts) 
		{
			if(map.containsKey(word))
			{
				map.put(word, map.get(word)+1);
			}
			else
				map.put(word, 1);
		}
		
		for (String word : map.keySet()) 
		{
			arg2.collect(new Text(word), new IntWritable(map.get(word)));
		}
		
	}
	
	
}
