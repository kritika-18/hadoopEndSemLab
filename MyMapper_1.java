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
		
	
		for(String num:data)
		{
			int number=Integer.parseInt(num);
			boolean falg=true;
			for(int i=2;i<=Math.sqrt(number);i++){
				if(number%i==0){
					flag=false;
					break;
				}
			}
			if(flag)
			{
				context.write(new Text("PrimeSum"), new IntWritable(number)); 
			}
			/*else{
				context.write(new Text("CompositeSum"), new IntWritable(number));
			}*/
			
			
					
		}
	}
}
