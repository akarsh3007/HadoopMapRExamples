

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class NetflixCategorizedRatingReducer  extends Reducer<Text, Text, Text, Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context ) throws InterruptedException
	{
		
		for(Text value : values)
		{
			String []data = value.toString().split(";");
			try {
				context.write(new Text(data[0]), new Text(data[1]));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
