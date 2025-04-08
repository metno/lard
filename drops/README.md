# DROPS

The DROPS module provides a framework for creating, updating, and retrieving products made from
observations.

DROPS stands for Demand-dRiven Observation Product System. The main reason it is called
demand-driven is that the design tries to make easy to define products that are only (re)computed
once there is

- a request for the product, or
- a change to the set of observations that the product is computed from.

Some products are computed on the fly while others rely on precomputed data.

Examples of products include:

- basic statistical aggregations (like maximum air temperature per month for a given station)
- climate normals
- climate records
- wind roses
- IDF data (Intensity, Duration, and Frequency) per station
- IDF data gridded over a large area
- precipitation so far in the current month
- combined time series (like the met.no/filter type in Frost v1)
- observations from a small set of stations to be read on radio (replacing the getobs.py script)

## Example

(**NOTE:** the sine wave and obs count product types are used only to demonstrate the API, the sine
wave type doesn't make use of any observations at all while the obs count merely returns the number
of observations within a time range for either a single station or all stations)

### Get a product

SineWave:

```text
$ curl -X POST -H 'content-type: application/json' 'localhost:3000/product/sinewave' -d '{"from_time":1,"to_time":20,"time_resolution":1,"min_value":-10,"max_value":10,"frequency":0.05}' -w '\n'
{"times":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19],"values":[0.0,3.0901699437494745,5.877852522924732,8.090169943749473,9.510565162951536,10.0,9.510565162951536,8.090169943749473,5.877852522924734,3.0901699437494745,1.7763568394002505e-15,-3.0901699437494763,-5.877852522924734,-8.090169943749473,-9.510565162951535,-10.0,-9.510565162951536,-8.09016994374947,-5.877852522924734]}
```

ObsCount:

```text
$ curl -X POST -H 'content-type: application/json' 'localhost:3000/product/obscount' -d '{"from_time":1,"to_time":20}' -w '\n'
{"obs_count":123}
```

### Get availability of one product type

SineWave

```text
$ curl -X GET 'localhost:3000/product/sinewave/availability' -w '\n'
{"description":"A basic sine wave.","input_instances":[],"input_schema":{"additionalProperties":false,"properties":{"frequency":{"description":"cycles per second","minimum":0,"type":"number"},"from_time":{"description":"earliest second","type":"integer"},"max_value":{"description":"maximum value","type":"number"},"min_value":{"description":"minimum value","type":"number"},"time_resolution":{"description":"seconds between values","minimum":1,"type":"integer"},"to_time":{"description":"latest second","type":"number"}},"required":["min_value"],"type":"object"},"output_schema":{"additionalProperties":false,"properties":{"times":{"items":{"type":"integer"},"type":"array"},"values":{"items":{"type":"number"},"type":"array"}},"type":"object"},"type":"SineWave"}
```

ObsCount:

```text
$ curl -X GET 'localhost:3000/product/obscount/availability' -w '\n'
{"name":"ObsCount","description":"A demo type that gets the number of observations in a time range for either one or all stations.","input_schema":{"additionalProperties":false,"properties":{"from_time":{"description":"earliest observation time (UNIX timestamp, default: -infinity)","type":"integer"},"station_ids":{"description":"contributing station ID (default = all stations)","type":"integer"},"to_time":{"description":"latest observation time (UNIX timestamp, default: infinity)","type":"integer"}},"type":"object"},"output_schema":{"additionalProperties":false,"properties":{"obs_count":{"minimum":0,"type":"integer"}},"type":"object"},"input_instances":[]}
```

### Get availability of all product types

```text
$ curl -X GET 'localhost:3000/product/availability' -w '\n'
[{"name":"SineWave","description":"A demo type that computes a sine wave.","input_schema":{"additionalProperties":false,"properties":{"frequency":{"description":"cycles per second","minimum":0,"type":"number"},"from_time":{"description":"earliest second (UNIX timestamp)","type":"integer"},"max_value":{"description":"maximum value","type":"number"},"min_value":{"description":"minimum value","type":"number"},"time_resolution":{"description":"seconds between values","minimum":1,"type":"integer"},"to_time":{"description":"latest second (UNIX timestamp)","type":"integer"}},"required":["from_time","to_time","time_resolution","min_value","max_value","frequency"],"type":"object"},"output_schema":{"additionalProperties":false,"properties":{"times":{"items":{"type":"integer"},"type":"array"},"values":{"items":{"type":"number"},"type":"array"}},"required":["times","values"],"type":"object"},"input_instances":[]},{"name":"ObsCount","description":"A demo type that gets the number of observations in a time range for either one or all stations.","input_schema":{"additionalProperties":false,"properties":{"from_time":{"description":"earliest observation time (UNIX timestamp, default: -infinity)","type":"integer"},"station_ids":{"description":"contributing station ID (default = all stations)","type":"integer"},"to_time":{"description":"latest observation time (UNIX timestamp, default: infinity)","type":"integer"}},"type":"object"},"output_schema":{"additionalProperties":false,"properties":{"obs_count":{"minimum":0,"type":"integer"}},"type":"object"},"input_instances":[]}]
```
