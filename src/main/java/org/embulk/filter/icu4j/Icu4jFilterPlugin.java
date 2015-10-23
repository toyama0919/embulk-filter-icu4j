package org.embulk.filter.icu4j;

import java.util.List;
import java.util.Map;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.ibm.icu.text.Transliterator;

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
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema, final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final List<Column> keyNameColumns = Lists.newArrayList();
        for (String keyName : task.getKeyNames()) {
            keyNameColumns.add(inputSchema.lookupColumn(keyName));
        }
        final List<List<Transliterator>> transliterators = Lists.newArrayList();
        for (Map<String, String> setting : task.getSettings()) {
            List<Transliterator> tokenizers = Lists.newArrayList();
            for (String convertType : setting.get("transliterators").split(",")) {
                Transliterator transliterator = Transliterator.getInstance(convertType);
                tokenizers.add(transliterator);
            }
            transliterators.add(tokenizers);
        }

        return new PageOutput() {
            private PageReader reader = new PageReader(inputSchema);
            private PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void finish() {
                builder.finish();
            }

            @Override
            public void close() {
                builder.close();
            }

            @Override
            public void add(Page page) {
                reader.setPage(page);
                while (reader.nextRecord()) {
                    if (task.getKeepInput()) {
                        for (Column inputColumn: inputSchema.getColumns()) {
                            if (reader.isNull(inputColumn)) {
                                builder.setNull(inputColumn);
                                continue;
                            }
                            if (Types.STRING.equals(inputColumn.getType())) {
                                builder.setString(inputColumn, reader.getString(inputColumn));
                            } else if (Types.BOOLEAN.equals(inputColumn.getType())) {
                                builder.setBoolean(inputColumn, reader.getBoolean(inputColumn));
                            } else if (Types.DOUBLE.equals(inputColumn.getType())) {
                                builder.setDouble(inputColumn, reader.getDouble(inputColumn));
                            } else if (Types.LONG.equals(inputColumn.getType())) {
                                builder.setLong(inputColumn, reader.getLong(inputColumn));
                            } else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
                                builder.setTimestamp(inputColumn, reader.getTimestamp(inputColumn));
                            }
                        }
                    }

                    List<Map<String, String>> settings = task.getSettings();
                    for (Column column : keyNameColumns) {
                        for (int i = 0; i < settings.size(); i++) {
                            Map<String, String> setting = settings.get(i);
                            String suffix = setting.get("suffix");
                            Column outputColumn = outputSchema.lookupColumn(column.getName() + MoreObjects.firstNonNull(suffix, ""));
                            String convert = convert(column, suffix, setting.get("case"), transliterators.get(i));
                            if (convert == null) {
                                builder.setNull(outputColumn);
                            } else {
                                builder.setString(outputColumn, convert);
                            }
                        }
                    }
                    builder.addRecord();
                }
            }

            /**
             * @param column
             * @param suffix
             * @param type
             * @return
             */
            private String convert(Column column, String suffix, String type, List<Transliterator> transliterators) {
                String string = reader.getString(column);
                for (Transliterator transliterator : transliterators) {
                    string = transliterator.transliterate(string);
                }
                if ("upper".equals(type)) {
                    string = string.toUpperCase();
                } else if ("lower".equals(type)) {
                    string = string.toLowerCase();
                }
                return string;
            }
        };
    }
}
