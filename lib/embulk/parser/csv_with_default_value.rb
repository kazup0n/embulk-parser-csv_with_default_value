Embulk::JavaPlugin.register_parser(
  "csv_with_default_value", "org.embulk.parser.csv_with_default_value.CsvWithDefaultValueParserPlugin",
  File.expand_path('../../../../classpath', __FILE__))
