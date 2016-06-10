package org.embulk.filter.icu4j;

import com.google.common.collect.Lists;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.filter.icu4j.Icu4jFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;

public class TestIcu4jFilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private Icu4jFilterPlugin plugin;

    @Before
    public void createReasource()
    {
        plugin = new Icu4jFilterPlugin();
    }

    private Schema schema(Column... columns)
    {
        return new Schema(Lists.newArrayList(columns));
    }

    private ConfigSource configFromYamlString(String... lines)
    {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append("\n");
        }
        String yamlString = builder.toString();

        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        return loader.fromYamlString(yamlString);
    }

    private PluginTask taskFromYamlString(String... lines)
    {
        ConfigSource config = configFromYamlString(lines);
        return config.loadConfig(PluginTask.class);
    }

    private void transaction(ConfigSource config, Schema inputSchema)
    {
        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
            }
        });
    }
}