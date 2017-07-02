import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class NetflixCategorizedRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	
	NetflixDataParser parser = new NetflixDataParser();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();

		parser.parse(line);
		if(parser.isRatingValid() && !parser.isHeader())
			context.write(new Text(String.valueOf(key)),new Text(parser.getRating() +";" + parser.getMovieName()));

	}

}
