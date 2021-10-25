package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackendFactory;

/** A factory that creates an {@link RedpandaStateBackend} from a configuration. */
@PublicEvolving
public class RedpandaStateBackendFactory
        implements StateBackendFactory<RedpandaStateBackend> {
    @Override
    public RedpandaStateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        return new RedpandaStateBackend().configure(config, classLoader);
    }
}
