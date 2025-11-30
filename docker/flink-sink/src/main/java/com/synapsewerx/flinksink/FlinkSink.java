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
        appConfigurations.forEach((key, value) -> {
            System.out.printf("%s=%s\n", key, value);
        });
        new SourceReader(env, appConfigurations).getRawStream();
        //String executionPlan = env.getExecutionPlan();
       env.execute("CFLT meetup sink");
        //System.out.println(cfltMeetupSink.getJobExecutionResult());
    }

}
