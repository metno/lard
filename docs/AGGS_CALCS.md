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
- In order to make certain types of daily aggregations there must be no missing data (e.g. if hourly need 24 observations)
- The data must be regularly spaced (aka none of the observations can have been significantly delayed) if there is a known time resolution

In order to deal with the missing data cutoff we count the number of observations in each bin, and compare the count to the 
required number for that time resolution. The regularlity can also be checked at the time of aggregation, if the
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

## Work that still needs to be done for covering all products made by triggers in KDVH

### Other aggregations, such as integral_of_excess and integral_of_deficit:
These aggregations should fit neatly in with the others:

Count values matching logical operation: < num | <= num | > num | >= num | IN(num1,num2,...)
Sum values exceeding threshold (integral_of_excess, integral_of_deficit)
That would complete the normal aggregators Ketil had reproduced in ODA. 
It includes GD17 which Ketil believes we are required to deliver for use in the building industry.

#### over_time elements: 
Ketil has noted that he is unsure how important these are, and that we might survive without them for a bit?

SLAG: 10051, over_time(precipitation_type P1D)
SLAGV: 10052, over_time(precipitation_class1 P1D)
SLAGW: 10053, over_time(precipitation_type_visual P1D)
SLAGWA: 10054, over_time(precipitation_type_automatic P1D)

The SLAGs are all diurnal summaries of the most important precipitation code value. For example, SLAGWA will check all 
automatic observations of coded weather type over the day, and return the code for hail if hail was seen during the day. 
If not, it will check whether both rain and snow were seen during the day and return the code for mixed precipitation. 
And so on. The logical structure can get verbose:
https://gitlab.met.no/oda/oda/-/blob/main/internal/pkg/abakus/scanners.go?ref_type=heads#L219

Importantly, the SLAGs do not usually care if some values are missing during the day. They will check whatever is available 
(which often varies from station to station). The trigger SQLs may be more useful than the ODA code in determining how to 
do this in SQL.

### P1M and P1Y
Based on daily aggregations, be able to calculate monthly and yearly aggregations. At least for avg, max, min, but the 
full list would possibly require checking all the triggers from KDVH or looking at what Ketil did here:
https://gitlab.met.no/oda/oda/-/blob/main/tools/metproducts/productsfull_add.txt?ref_type=heads
