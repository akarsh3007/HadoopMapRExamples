import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;

public class NetFlixTopRatedShowsReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
	{
		
		TreeMultimap<Integer, String> topShows = TreeMultimap.create(Ordering.natural().reverse(),Ordering.natural());
		StringBuilder sb = new StringBuilder();
		for (Text value : values) {
			
			String [] data = value.toString().split(";");
			if(data.length == 2)
			{				
				System.out.println(data[1] + " " + data[0]);
				topShows.put(Integer.parseInt(data[1]), data[0]);
				
			}
			
		}
		 
		int count = 0;

		sb.append(System.lineSeparator());
		for (String movie : topShows.values()) {
			if(count == 10)
				break;
			sb.append(movie);
			sb.append(System.lineSeparator());
			count++;
		}
		
	
		
		try {
			context.write(new Text("Top Rated Movies"), new Text(sb.toString()));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
