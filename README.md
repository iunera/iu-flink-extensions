# iu-flink-extensions
Several Flink common flink helpers can be found in this project. Those are in special:

## Window functions
Flink supports Event Time and Processing Time windows.
That is an OR what means one can adjust to the event time or the processing time.
Event time means that as long as there is no element arriving, it does not matter how much real time between the data has passed.
This can be problematic when one wants to aggregate/group the event data packages and when there is no data package arriving for a minute the aggregation shall be triggered.

### Problems
**1** Imagine a late event at processing time having an older event time than than a priorly processed event. In this case, the trigger will wait forever. For this reason, this trigger offers a fallback to processing time, that waits the window time plus an out of orderness value to process.

**2** When an event in a stream does not have a succeeding event and one processes by event time, the firing of the window will wait till there is a newer event with a more recent timestamp. This is also solved by waiting the event time for a new event.

**Use Case Example:**
Imagine there are people count in and count out events at a busstop. The aggregation of all events would be the total count in and total count out. Sometimes there is an event indicating the bus departed what means the aggregation could be done. However, there is not always a clear indicator for the departure and sometimes data packages are left out, in disorder or arriving really late (e.g. hours). Therefore, we need the ability to trigger the aggregation one minute after the last event for the aggregation was received, because then we manage to get 99% of all cases covered, regardless if data is much later. 

## Solution Combined Window Function
Solution is the window function and the trigger allowing to set max out of orderness of events based on event and processing time in combination. In combination with a suiting trigger function optional processing near real time can be achieved.

## Geotrigger for the Window Functions
This allows window function triggers triggers to be fired once a geolocation changes. Imagine you want to start processing forecasts how many people will arrive at the next bus stop and if the bus is empty of full. With this trigger one can get all door in and out events of passengers and once the bus changes the geoposition one knows that it is departed and processing can start. Likewise is for other traffic scenarios such a trigger can aggregate and reduce (big) data by firing triggers for aggregation of the data once a geopsoition changed. This way, fine grained geoposition data can be aggregated into a forced grained tile of geoposition data.

# Pojo helpers
There are a few helpers to work with Pojos in Flink to use the advantages of static typed variables and their semantic names in combination with flexible processing. 
Those are:

## Copy all fields
Copies all fields from one source class to a target class based on the name. 
This makes it possible to use the same field names in the target class than in the source without having a necessity for inheritance.Optionally, certain fields can be ignored when they are specified in the constructor.
This is useful when one uses different data formats with shared elements.

## Pojo Field Copy - Mapping
Same like Copy all fields, but with the capability to have a mapping between the names of fields.
This enables data model and Pojo updates or to transform between between different data models with different names.

## CSV Serializer
Allows to serialize a POJO to a CSV with the headline of the column names as the column name.

## Pojo Json Serializer
Serializes Pojos into Json

# Usage
Implemeted in your pom.xml the following dependency
```
<dependency>
    <groupId>com.iunera.fahrbar</groupId>
    <artifactId>iu-flink-extensions</artifactId>
    <version>2.0.0</version>
</dependency>
```



# License
[Open Compensation Token License, Version 0.20](https://github.com/open-compensation-token-license/license/blob/main/LICENSE.md)
