import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.operation.aggregator.*;
import cascading.pipe.*;
import cascading.property.AppProps;
import cascading.scheme.hadoop.*;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class WordCount {
	
	public static void main(String[] args) {
		
		
		if(args.length !=2 )
		{
			System.err.print("Invalid Arguments, Please provide input and output arguments");
			return ;
		}
		
		String inputPath = args[0];
		String outputPath = args[1];
		
		Properties properties = new Properties();
		AppProps.setApplicationName(properties, "WordCount Cascading");
		AppProps.setApplicationJarClass(properties, WordCount.class);
		AppProps.addApplicationTag(properties, "Word Count");
		
		FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);
		
		Tap inTap = new Hfs(new TextDelimited(true, "\t"), inputPath);
		
		Tap wcTap = new Hfs(new TextDelimited(true, "\t"), outputPath);
		
		Fields token = new Fields("token");
		
		Fields text = new Fields("text");
		
		
		RegexSplitGenerator splitter = new RegexSplitGenerator(token,"[ \\[\\]\\(\\),.]");
	    Pipe docPipe = new Each( "token", text, splitter, Fields.RESULTS );

	    // determine the word counts
	    Pipe wcPipe = new Pipe( "wc", docPipe );
	    wcPipe = new GroupBy( wcPipe, token );
	    wcPipe = new Every( wcPipe, Fields.ALL, new Count(), Fields.ALL );

	    FlowDef flowDef = FlowDef.flowDef().setName( "wc" ).addSource( docPipe, inTap ).addTailSink( wcPipe, wcTap );	  
	    Flow wcFlow = flowConnector.connect( flowDef );
	    
	    wcFlow.writeDOT( "dot/wc.dot" );
	    wcFlow.complete();
		
		
		
		
	}
}
