This project contains throwaway code for a proof of concept.

The `LayerCrawler` takes a series of coordinates and runs them against the Australian layer service. This returns the various layers that each coordinate joins with.
The `LayerLoader` then puts the `LatLng` and JSON object into a GBIF `KeyValue` complaint HBase cache. That cache can then be used for a "get layers by coordinate" service.

All of this is only for proof of concept.

To get the Australia points from GBIF:
```
create table tim.au_coords
ROW FORMAT DELIMITED fields terminated by ','
as
select distinct decimalLatitude, decimalLongitude
from prod_f.occurrence_hdfs
where countryCode='AU'
and hasgeospatialissues = false
```

To create the table in HBase:
```
create 'australia_kv', {NAME => 'v', BLOOMFILTER => 'ROW', DATA_BLOCK_ENCODING => 'FAST_DIFF', COMPRESSION => 'SNAPPY'},{SPLITS => ['1','2','3','4','5','6','7','8','9']}
```