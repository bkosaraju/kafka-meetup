package com.synapsewerx.flinksink;

import com.synapsewerx.flinksink.utils.FlinkUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;

public class FlinkSink {
    public FlinkSink() {
        this.appConfig = appConfig;
    }

    public void setAppConfig(String appConfig) {
        this.appConfig = appConfig;
    }

    String appConfig;

    public void startApp() throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.createStreamExecutionEnvironment();
        HashMap<String, String> appConfigurations = FlinkUtils.getResourceConfigurations(appConfig);
        new DynamicSourceReader(env, appConfigurations).getRawStream();
        env.execute("Kafka meetup sink");
    }

}
