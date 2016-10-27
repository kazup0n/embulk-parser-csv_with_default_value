# Csv With Default Value parser plugin for Embulk

This plugin behaves as same as standard csv parser, but default values can be specified with this plugin.

- Default values can be specified with each columns.
- Default values are used with each

## Overview

* **Plugin type**: parser

Default values of each columns are used when parsing original data is failed.

## Configuration

- **default_values**: default values for each columns (map optional)
    - type: `immediate`(default) or `'null'`
    - default_value: default value as string (required when `type` is `immediate`)

Default values can be specified to only long, double and timestamp.
(`type: null` is not allowed for long and double)

## Example

```yaml
in:
  type: file
  parser:
    type: csv_with_default_value
    delimiter: ','
    header_line: false
    columns:
    - {name: stringCol, type: string}
    - {name: longCol, type: long}
    - {name: doubleCol, type: double}
    - {name: timestampCol, type: timestamp, format: '%Y-%m-%d %H:%M:%S'}
    default_values:
        longCol: {type: 'null'}
        doubleCol: {type: 'null'}
        timestampCol: {default_value: '2000-12-01 12:00:00'}
```




```
$ embulk gem install embulk-parser-csv_with_default_value
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
