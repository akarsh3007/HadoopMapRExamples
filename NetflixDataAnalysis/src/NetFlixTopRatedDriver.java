import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NetFlixTopRatedDriver  {
	
	public static void main(String[] args) {
		
		if(args.length !=2)
		{
			System.err.println("Invalid input format. Provide input and output path");
		}
		Job job = null;
		try {
			job = Job.getInstance();
			job.setJarByClass(NetFlixTopRatedDriver.class);
			job.setJobName("NetFlix Top Rated Shows");
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setMapperClass(NetflixTopRatedShowsMapper.class);
			job.setReducerClass(NetFlixTopRatedShowsReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
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
