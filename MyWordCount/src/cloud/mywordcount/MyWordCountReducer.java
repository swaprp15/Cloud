package cloud.mywordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MyWordCountReducer implements Reducer<Text, IntWritable, Text, IntWritable>
{
/*
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void reduce(Text reduceKey, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		
		int totalCount = 0;
		
		Iterator<IntWritable> it = values.iterator();
		
		while(it.hasNext())
		{
			totalCount += it.next().get();
		}
		
		context.write(reduceKey, new IntWritable(totalCount));
	}

	@Override
	public void run(Context arg0)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.run(arg0);
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
 */

	@Override
	public void reduce(Text reduceKey, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
		int totalCount = 0;
		
		Iterator<IntWritable> it = values;
		
		while(it.hasNext())
		{
			totalCount += it.next().get();
		}
		
		arg2.collect(reduceKey, new IntWritable(totalCount));
		
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
