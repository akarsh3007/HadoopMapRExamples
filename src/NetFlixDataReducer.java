import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NetFlixDataReducer  extends Reducer<Text, IntWritable, Text, IntWritable>{

	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	{
		
		int count = 0;
		for (IntWritable value : values) {
			
			count+= value.get();
		}
		try {
			context.write(key, new IntWritable(count));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
