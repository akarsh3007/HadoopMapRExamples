import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NetFlixDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	NetflixDataParser parser = new NetflixDataParser();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		
		
		boolean isParsed = parser.NetflixDataParse(line);
		if(isParsed)
		{
				context.write(new Text(parser.getYear()), new IntWritable(1));
			
		}

	}

}
