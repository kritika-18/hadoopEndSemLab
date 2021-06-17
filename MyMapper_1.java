// exception handling
import org.apache.hadoop.io.IntWritable;

//import box classes
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

//import mapper class
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MyMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> 
{

	
	public void map(LongWritable key, Text value, Context context)throws java.io.IOException, InterruptedException
	{
		String data[]=value.toString().split(",");    
		int mul=0;
	
		for(String num:data)
		{
			int number=Integer.parseInt(num);
			for(int i=2;i<(number/2);i++){
				if(number%i==0){
					mul=1;
					break;
				}
			}
			if(mul==0)
			{
				context.write(new Text("PrimeSum"), new IntWritable(number));   //  ODD 85  131  993 
			}
			
			
			
					
		}
	}
}