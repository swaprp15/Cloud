package cloud.invertedindex;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InvertedIndexReducer implements Reducer<Text, Text, Text, Text> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reduce(Text reduceKey, Iterator<Text> it,
			OutputCollector<Text, Text> collector, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		
		StringBuilder websites = new StringBuilder();
		
		if(it.hasNext())
		{
			websites.append(it.next());
		}
		
		while(it.hasNext())
		{
			websites.append(",").append(it.next());
		}
		
		collector.collect(reduceKey, new Text(websites.toString()));
	}

}
