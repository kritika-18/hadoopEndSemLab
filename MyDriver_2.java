import java.io.IOException;

//file import
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//box class import
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

//mapredue import
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver2 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException 
	{
		
		Job j = new Job();
		j.setJobName("My First Job");
		j.setJarByClass(MyDriver2.class );
		j.setMapperClass(MyMapper2.class );
		
		j.setReducerClass(MyReducer2.class);
		
		
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		
		
		
		
		System.exit(j.waitForCompletion(true) ? 0 : 1);
		
	}

}



