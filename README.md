# csv-exporter
## Purpose
This is a utility which allows some or all of the data present in an Aerospike repository to be exported to .CSV files

### Usage
```
usage: com.aerospike.exporter.CsvExporter [<options>]
options:
-A,--maxSize <arg>    Maximum size of records to print (default: unlimited)
-d,--directory <arg>  Output directory (default: .)
-f,--from <arg>       From time (accepts dd/MM/yyyy-hh:mm:ss) (default: null)
-h,--host <arg>       Server hostname (default: 127.0.0.1)
-I,--minSize <arg>    Minimum size of records to print (default: 0)
-l,--limit <arg>      Limit for the number of records. 0 for unlimited. (default: 0)
-m,--metadata <arg>   Whether to record metadata in the records or not (default: false)
-n,--namespace <arg>  Comma separated list of Namespaces to print, or omit for all sets (default:
                      none, print all namespace)
-p,--port <arg>       Server port (default: 3000)
-P,--password <arg>   Password for secured clusters (optional)
-s,--set <arg>        Comma separated list of Sets to print, or omit for all sets (default: none,
                      print all sets)
-t,--to <arg>         To time (accepts dd/MM/yyyyhh:mm:ss) (default: null)
-u,--usage            Print usage
-U,--user <arg>       User for secured clusters (optional)
```

The namespaces and sets are optional -- if present, only sets in the set list which are present in the namespaces in the namespace list will be exported.

### Known Issuses
* At the moment there is no way to export the null set with any namespace
* The code has been tested with numeric and string data only. List and map data is likely to work, BLOB, bitmap and GeoJSON data times require some further work. 
