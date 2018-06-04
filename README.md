# Big data analytics 2018

## 06 GEOSPATIAL AND TEMPORAL DATA ANALYSIS ON THE NEW YORK CITY TAXI TRIP DATA
Summary

The goal is to calculate the utilization of New York city taxi cabs: the fraction of time that a cab is on the road and is occupied by one or more passengers and is generating income for the taxi driver. The utilization varies by borough (Manhattan, Brooklyn, Queens, ...) and is thus a function of the passenger's destination.

The dataset comes from the city of New York which tracks the GPS position of each taxi. It contains a list of rides. For each ride there is information about the taxi and the driver, the time the trip started and ended and the GPS coordiantes where the passenger(s) were picked up and where they were dropped off.

Analysis uses specialized libraries for doing arithmetic with temporal data and mapping GPS coordinates to boroughs. A sessionazation algorithm allows to identify the behavior of an individual taxi driver throughout his/her day.
