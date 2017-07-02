import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NetflixTopRatedShowsMapper extends Mapper<LongWritable, Text, Text, Text> {

	NetflixDataParser parser = new NetflixDataParser();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		StringBuilder sb = new StringBuilder();

		parser.parse(line);
		sb.append(parser.getMovieName());
		sb.append(";");
		sb.append(parser.getRating());
		if (parser.isRatingValid() && !parser.isHeader()) {

			context.write(new Text("1"), new Text(sb.toString()));
		}

	}

}
