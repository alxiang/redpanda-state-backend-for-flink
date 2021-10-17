package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackendFactory;

/** A factory that creates an {@link RedpandaStateBackendFactory} from a configuration. */
@PublicEvolving
public class RedpandaStateBackendFactory
        implements StateBackendFactory<RedpandaStateBackendFactory> {
    @Override
    public RedpandaStateBackendFactory createFromConfig(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        return new RedpandaStateBackendFactory().configure(config, classLoader);
    }
}
