package org.embulk.filter.icu4j;

import java.util.List;
import java.util.Map;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

public class Icu4jFilterPlugin implements FilterPlugin
{
    public interface PluginTask extends Task
    {
        @Config("key_names")
        public List<String> getKeyNames();

        @Config("keep_input")
        @ConfigDefault("true")
        public boolean getKeepInput();

        @Config("settings")
        public List<Map<String, String>> getSettings();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        control.run(task.dump(), buildOutputSchema(task, inputSchema));
    }

    /**
     * @param inputSchema
     * @param task
     * @return
     */
    private Schema buildOutputSchema(PluginTask task, Schema inputSchema) {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;
        if (task.getKeepInput()) {
            for (Column inputColumn: inputSchema.getColumns()) {
                Column outputColumn = new Column(i++, inputColumn.getName(), inputColumn.getType());
                builder.add(outputColumn);
            }
        }

        for (String key: task.getKeyNames()) {
            for (Map<String, String> setting : task.getSettings()) {
                String keyName = key + MoreObjects.firstNonNull(setting.get("suffix"), "");
                if (task.getKeepInput()) {
                    if (setting.get("suffix") != null) {
                        builder.add(new Column(i++, keyName, Types.STRING));
                    }
                } else {
                    builder.add(new Column(i++, keyName, Types.STRING));
                }
            }
        }
        Schema outputSchema = new Schema(builder.build());
        return outputSchema;
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema, final Schema outputSchema, final PageOutput output)
    {
        return new Icu4jPageOutput(taskSource, inputSchema, outputSchema, output);
    }
}
