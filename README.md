Weather Integrator
=========
[![Travis CI](https://travis-ci.org/nkoutroumanis/Weather-Integrator.svg?branch=master)](https://travis-ci.org/nkoutroumanis/Weather-Integrator)<br/>

This work provides a mechanism for integrating weather data in files (such as csv) that contain information about Absolute Time and Coordinates (longitude and latitude) of a location.

Weather data is exported from [Grib files](http://weather.mailasail.com/Franks-Weather/Grib-Files-Explained)
(in a few words, a grib file contains weather data - features about weather for a certain time), and then is integrated in recored of files (csv, txt etc.)

A file can be enriched with weather data only if its records contain information about Absolute Time (Date & Time) and Coordinates (longitude and latitude).

