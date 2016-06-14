package org.embulk.filter.icu4j;

import java.util.List;
import java.util.Map;

import org.embulk.config.TaskSource;
import org.embulk.filter.icu4j.Icu4jFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.ibm.icu.text.Transliterator;

public class Icu4jPageOutput implements PageOutput
{
    private final PluginTask task;
    private final List<Column> keyNameColumns;
    private final List<List<Transliterator>> transliteratorsList;
    private final PageReader reader;
    private final PageBuilder builder;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private static final Logger logger = Exec.getLogger(Icu4jFilterPlugin.class);

    public Icu4jPageOutput(TaskSource taskSource, Schema inputSchema, Schema outputSchema, PageOutput output) {
        this.task = taskSource.loadTask(PluginTask.class);
        this.keyNameColumns = Lists.newArrayList();
        this.transliteratorsList = Lists.newArrayList();
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;

        for (String keyName : task.getKeyNames()) {
            keyNameColumns.add(inputSchema.lookupColumn(keyName));
        }
        for (Map<String, String> setting : task.getSettings()) {
            List<Transliterator> tokenizers = Lists.newArrayList();
            for (String convertType : setting.get("transliterators").split(",")) {
                Transliterator transliterator = Transliterator.getInstance(convertType);
                tokenizers.add(transliterator);
            }
            transliteratorsList.add(tokenizers);
        }
        reader = new PageReader(inputSchema);
        builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
    }

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
                    } else if (Types.JSON.equals(inputColumn.getType())) {
                        builder.setJson(inputColumn, reader.getJson(inputColumn));
                    }
                }
            }

            List<Map<String, String>> settings = task.getSettings();
            for (Column column : keyNameColumns) {
                for (int i = 0; i < settings.size(); i++) {
                    Map<String, String> setting = settings.get(i);
                    String suffix = setting.get("suffix");
                    Column outputColumn = outputSchema.lookupColumn(column.getName() + MoreObjects.firstNonNull(suffix, ""));
                    final String source = reader.getString(column);
                    final List<Transliterator> transliterators = transliteratorsList.get(i);
                    String converted = convert(source, suffix, setting.get("case"), transliterators);
                    logger.debug("before => [{}], after => [{}]", source, converted);
                    if (converted == null) {
                        builder.setNull(outputColumn);
                    } else {
                        builder.setString(outputColumn, converted);
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
    private String convert(String string, String suffix, String type, List<Transliterator> transliterators) {
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
}
