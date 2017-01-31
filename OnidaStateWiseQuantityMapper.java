package acadgild;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OnidaStateWiseQuantityMapper 
	extends Mapper<LongWritable, Text, Text, IntWritable>{

	public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
		String[] line = value.toString().split("\\|");
		String companyName = line[0];
		String stateName = line[3];
		if(companyName.equals("Onida")){
			System.out.println(stateName +" - 1");
			context.write(new Text(stateName), new IntWritable(1));
		}
	}
}
