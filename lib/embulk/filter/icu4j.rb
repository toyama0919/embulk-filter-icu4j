Embulk::JavaPlugin.register_filter(
  "icu4j", "org.embulk.filter.icu4j.Icu4jFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
