import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

public class NetflixCategorizedRatingPartitioner extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		
		String []values = value.toString().split(";");
		int rating = 0 ;
		rating = Integer.parseInt(values[0]);
		
		if(numPartitions == 0)
			return 0;
		if(rating >= 90)
			return 0;
		if(rating >=80 && rating < 90 )
			return 1 % numPartitions;
		else
			return 2 % numPartitions;
		
	}
	
	

}
