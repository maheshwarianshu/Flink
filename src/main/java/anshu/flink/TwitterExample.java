package anshu.flink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.apache.sling.commons.json.xml.XML;


public class TwitterExample {

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		
		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setLatencyTrackingInterval(5);

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		env.setParallelism(params.getInt("parallelism", 2));

		// get input data
		DataStream<String> streamSource;

			streamSource = env.addSource(new CustomSource(params));

		DataStream<String> tweets = streamSource.map(new JSONToXMLMap(params));
					
				// selecting English tweets and splitting to (word, 1)
			//	.flatMap(new SelectEnglishAndTokenizeFlatMap())
				// group by words and sum their occurrences
			//	.keyBy(0).sum(1);

		// emit result
		
		if (params.has("output")) {
			tweets.writeAsText(params.get("output"),FileSystem.WriteMode.OVERWRITE);
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			tweets.print();
		}
		env.execute("Twitter Streaming Example");
	}
}

class JSONToXMLMap implements MapFunction<String,String>{
	
	ParameterTool params;
	public JSONToXMLMap(ParameterTool params) {
		this.params = params;
	}

	private static final long serialVersionUID = 1L;

	@Override
	public String map(String json) throws Exception {
		
		 String output = null;
		 if(params.has("xml")){
			try {
				JSONObject j = new JSONObject(json);
				output = XML.toString(j);
				long sleep = params.getLong("sleep", 0);
				System.out.println("Use --sleep <time_in_millisec>, Will sleep for "+ sleep + " millisec");
				Thread.sleep(sleep);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		   }
		   else{
			   output = json;
		   }
		return output;
	}
}