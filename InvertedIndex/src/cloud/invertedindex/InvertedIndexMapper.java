package cloud.invertedindex;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class InvertedIndexMapper implements Mapper<LongWritable, Text, Text, Text> 
{

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
			OutputCollector<Text, Text> collector, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
		String input = value.toString();
		
		String parts[] = input.split(",");
		
		Set<String> products = new HashSet<String>();
		
		for(int i = 1; i < parts.length; i++)
		{
			products.add(parts[i]);
		}
		
		Iterator<String> it = products.iterator();
		
		Text website = new Text(parts[0]);
		
		while(it.hasNext())
		{
			collector.collect(new Text(it.next()), website);
		}
		
	}

}
