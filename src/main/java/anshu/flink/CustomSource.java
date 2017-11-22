package anshu.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.sling.commons.json.*;
import org.apache.sling.commons.json.xml.*;
import com.esotericsoftware.minlog.Log;
import com.satori.rtm.*;
import com.satori.rtm.model.*;

public class CustomSource extends RichSourceFunction<String>{

	private static final long serialVersionUID = 1L;
	RtmClient client = null;
	static final String endpoint = "wss://open-data.api.satori.com";
	static final String appkey = "b7E9E1D1f6aB38B1CeCeF762074CBc9b";
	static final String channel = "world";
	ParameterTool params = null;
	
	public CustomSource(ParameterTool params) {
		this.params = params;
	}
	@Override
	public void cancel() {
		Log.info("Cancelling");
	}

	@Override
	public void run(final SourceContext<String> ctx)
		throws Exception {
	    client = new RtmClientBuilder(endpoint, appkey)
	    		 .setListener(new RtmClientAdapter() {
	    			 @Override
			         public void onEnterConnected(RtmClient client) {
			            System.out.println("Connected to Satori RTM!");
			         }
	    		 })
	    		 .build();

	    SubscriptionAdapter listener = new SubscriptionAdapter() {
		    @Override
		    public void onSubscriptionData(SubscriptionData data) {
		       for (AnyJson json : data.getMessages()) {

		           ctx.collect(json.toString());
		       }
		    }
		};
	    
	    client.createSubscription(channel, SubscriptionMode.SIMPLE, listener);
	    client.start();

	    while(true){}
	}
}