package acadgild;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CompanyWiseQuantityReducer
	extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int count = 0;
		System.out.println("Company Name = "+key);
		System.out.println("Values = "+values);
		for (IntWritable val : values) {
			count+=val.get();
		}
		System.out.println(key+" - "+count);
		context.write(key, new IntWritable(count));
	}

}
