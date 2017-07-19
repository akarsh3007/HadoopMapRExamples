package akarsh.netflix.analysis;
import java.util.Properties;

import com.tresata.cascading.opencsv.OpenCsvScheme;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.aggregator.Count;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.*;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class NetflixShowCountYearWise {

	public static void main(String[] args) {

		if (args.length != 2) {
			System.err.println("Invalid input format. Please provide input and output path");
			return;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		Properties properties = new Properties();
		AppProps.setApplicationName(properties, "Netflix Show Count Year Wise");
		AppProps.setApplicationJarClass(properties, NetflixShowCountYearWise.class);
		AppProps.addApplicationTag(properties, "Netflix");
		AppProps.addApplicationTag(properties, "Show Count by Year");

		FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);
		
		Tap inTap = new Hfs(new OpenCsvScheme(',','"','\\',false), inputPath);

		Tap outTap = new Hfs(new TextDelimited(true, "\t"), outputPath);
		
		Fields releaseYear = new Fields("release year");

		Pipe yearPipe = new Pipe("year");
		yearPipe = new GroupBy(yearPipe,releaseYear);
		yearPipe = new Every(yearPipe,releaseYear,new Count(),Fields.ALL);
		
		FlowDef flowDef = FlowDef.flowDef().setName( "Netflix Show Count By Year" ).addSource(yearPipe, inTap ).addTailSink( yearPipe, outTap);	  
	    Flow nfscFlow = flowConnector.connect( flowDef );
	    
	    nfscFlow.writeDOT( "dot/NetflixShowCountByYear.dot" );
	    nfscFlow.complete();
	}
}
