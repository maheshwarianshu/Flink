package anshu.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


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

		DataStream<String> tweets = streamSource.broadcast();
				// selecting English tweets and splitting to (word, 1)
			//	.flatMap(new SelectEnglishAndTokenizeFlatMap())
				// group by words and sum their occurrences
			//	.keyBy(0).sum(1);

		// emit result
		if (params.has("output")) {
			tweets.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			tweets.print();
		}
		env.execute("Twitter Streaming Example");
	}


}