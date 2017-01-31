package acadgild;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CompanyWiseQuantityMapper extends
	Mapper<LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
			String[] line = value.toString().split("\\|");
			String companyName = line[0];
			if(!companyName.equals("NA")){
				System.out.println("Company Name - "+companyName);
				context.write(new Text(companyName), new IntWritable(1));
			}
		}
}