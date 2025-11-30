package com.synapsewerx.flinksink;

public class Launcher {
    public static void main(String[] args) throws Exception {
        FlinkSink flinkSink = new FlinkSink();
        if (args.length >0 ) {
         flinkSink.setAppConfig(args[0]);
        }
        flinkSink.startApp();
}
}
