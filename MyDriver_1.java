import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver1 
{
	public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException 
	{

		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "PrimeSum");  
		
		
		job.setJarByClass(MyDriver1.class);
		
		job.setMapperClass(MyMapper1.class);
		
		job.setReducerClass(MyReducer1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}