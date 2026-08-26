# Aggregations and Calculations

We have several goals with aggregations and calculations (products derived from the data ingested into LARD):
1. Be able to generically create aggregations on a timeseries 
   (most likely on some form of patchwork in order to supply the users with longer timeseries)
2. Be able to reproduce the specific aggregations that KDVH creates (aka 18H/6H time offsets on one particular timeresolution)
*Future* (and possibly not directly in the Lard system):
3. Eventually be able to replace kvalobs as the source of sending realtime data to GTS (including aggregations)
4. Are hourly aggregations being made by kvalobs? We need to reproduce this... (negative typeids)

The KDVH aggregations enforce the climate divisions requirements that may or may not be desired by all use groups.
These requirements include:
- In order to make a daily aggregation there must be no missing data (e.g. if hourly need 24 observations)
- The data must be regularly spaced (aka none of the observations can have been significantly delayed)

In order to deal with the missing data cutoff we count the number of observations in each bin, and compare the count to the 
required number for that time resolution. The regularlity can also potentially be checked at the time of aggregation, if the
expected time resolution is used as a rule. 

Other notes:
UTC is used for aggregations (but norwegian time changes in relation to that)

## Goals for end users, what do they want?
_Climate:_ wants to continue the the timeseries they have always had 
        daily, and then monthly, yearly, seasons (which are based on the daily aggregations)
_WMO / Forecasting:_ wants timeseries with regular time resolution (hourly for now, or moving towards higher resolution ...)
_Weather visualization / media:_ 0-0 aggregations (no offset)? Information about quality? 

## Caching / storage?
In terms of testing the need to store/cache daily data, we need to see how fast it is to aggregate:
100 stations (MET) for the last 100 years

Potential for caching / storing certain timeseries (particularly hourly)
- If we always ensured that we stored hourly, daily, and this is the basis of many calculations ... 
  would that simplify our life?
- Will caching be needed for sending data to WMO? Or just a cron of some sort?
