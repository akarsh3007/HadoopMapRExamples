import java.util.Properties;

import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.*;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

public class CopyFile {

	
	public static void main(String[] args) {
		
//		if(args.length !=2 )
//		{
//			System.err.println("Invalid Input Format");
//			return ;
//		}
//		
		String inputPath = args[0];
		String outputPath = args[1];
		
		Properties props = new Properties();
		
		AppProps.setApplicationName(props, "Cascding Copy File");
		AppProps.setApplicationJarClass(props, CopyFile.class);
		AppProps.addApplicationTag(props, "Cascading");
		
		Hadoop2MR1FlowConnector flowConnector = new Hadoop2MR1FlowConnector(props);
		
		// create an Input Tap 
		
		Tap inTap = new Hfs(new TextDelimited(true, "\t"), inputPath);
		
		// create an output Tap 
		
		Tap outTap = new Hfs(new TextDelimited(true, "\t"), outputPath);
		
		Pipe copyPipe = new Pipe("copy file");
		
		FlowDef flowDef = new FlowDef().flowDef().addSource(copyPipe, inTap).addTailSink(copyPipe, outTap);
		
		flowConnector.connect(flowDef).complete();
	}
	
}