import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NetflixDataDriver extends Configured {
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		if(args.length !=2)
		{
			System.err.println("Invalid input format. Provide input and output path");
		}
		Job job = null;
		try {
			job = Job.getInstance();
			job.setJarByClass(NetflixDataDriver.class);
			job.setJobName("NetFlix Show Count By Year");
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setMapperClass(NetFlixDataMapper.class);
			job.setReducerClass(NetFlixDataReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			System.exit(job.waitForCompletion(true) ?0:1);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}


}
