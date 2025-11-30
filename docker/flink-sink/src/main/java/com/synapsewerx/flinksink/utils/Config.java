package com.synapsewerx.flinksink.utils;

public final class Config {
    private Config() {
        throw new UnsupportedOperationException("This is a configuration class and should not be instantiated.");
    }
    public static final long DEFAULT_CHECKPOINT_INTERVAL = 12000L;
    public static final long MIN_PAUSE_CHECKPOINT_INTERVAL = 12000L;
    public static final long CHECKPOINT_TIMEOUT = 120000L;
    public static final int TOLERABLE_CHECKPOINT_FAILURES = 3;
    public static final int MAX_CONCURRENT_CHECKPOINTS = 1;
    public static final String CHECKPOINT_STORAGE = "file:///tmp/flink/checkpoint";
    public static final String APP_PROPERTIES = "app.properties";
}