# DROPS

The DROPS module provides functions for creating, updating, and retrieving products made from
observations.

DROPS stands for Demand-dRiven Observation Product System. The main reason it is called
demand-driven is that the design tries to make easy to define products that are only (re)computed
once there is

- a request for the product, or
- a change to the set of observations that the product is computed from.

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
