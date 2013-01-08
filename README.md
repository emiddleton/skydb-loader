# Skydb Loader - load snowplow data into skydb

### Introduction

This is a proof of concept implimentation of a data loader for [snowplow][snowplow] data into a [Skydb open source behavioral database] [skydb] database.

The module breakup and function signatures share a similar structure to [Infobright ruby loader] [infobright-loader].

### Getting Started

1. setup and start a skydb database.
2. set snowplow data directory and database directory in bin/skydb-loader

  config.folder to snowplow data director
  config.db database directory

3. run bin/skydb-loader

[skydb]: http://skydb.io/
[infobright-loader]: https://github.com/snowplow/infobright-ruby-loader
[snowplow]: https://github.com/snowplow/snowplow
