Weather Integrator
=========
[![Travis CI](https://travis-ci.org/nkoutroumanis/Weather-Integrator.svg?branch=master)](https://travis-ci.org/nkoutroumanis/Weather-Integrator)<br/>

Data integrator implementation in java for integrating weather information datasets composed of spatio-temporal points.

This work provides a mechanism for enriching spatio-temporal point records (records that contain longitude, latitude and date information) with weather data;
attributes that describe characteristics of the prevailing weather conditions, such as temperature. The weather data can be found in GRIB files that store the values 
of weather attributes in serialized form and refer to a specific time period and to a specific geographical area (or the whole globe). 

Features                                                                                                                        
-                                                                                                                               
* The records of spatio-temporal points (dataset) that are to be enriched, may be stored in files (such as CSV) or in a Kafka topic  
* The records of spatio-temporal points (dataset) that are to be enriched may be either in JSON or in delimiter separated format
* The output result (enriched records) may be written either in files or in Kafka topic                                         
* The weather data source can be accessed either locally or via HDFS during the integration procedure            

What are GRIB files and where can I find them? 
-                                              
GRIB is a standardized binary data format by the World Meteorological Organization's Commission for Basic Systems, commonly used in meteorology to store historical and forecast weather data.
Publicly GRIB files with forecast weather data are offered by [NoAA](https://www.ncdc.noaa.gov/data-access/model-data/model-datasets/global-forcast-system-gfs) and [ECMWF](https://apps.ecmwf.int/datasets/) and are freely available for download.
Other organizations may also distribute for free weather data sources in the form of GRIB files.

What GRIB files should I choose for enriching my dataset with weather data?
-                                             
When choosing the GRIB files as the weather data source for the enrichment procedure, it is preferable to opt for those with the lowest grid scale for better accuracy.
Also, since 4 models are run per day each one starting at 00:00, 06:00, 12:00 and 18:00, prefer the files for each model with steps 000 or 003 or 006.

The step number of a grib file declares the hours ahead from the starting time of the model. The contained weather information is referring to that time ahead. 
GRIB files with steps > 000, contain both instantaneous weather attributes and average variables for some passed time. For instance, a GRIB file with step 006 
contain weather attributes that refer to 6-hours ahead from the starting time of the model and 6-hour average attributes that refer to the passed time period. 
A GRIB file with step 000 contains only instantaneous attributes.

_Important Note:_ be sure to download the GRIB files that will cover the spanning time period of the spatio-temporal points of your dataset! 
 
How can I see the available weather attributes of a GRIB file so as to choose what information to integrate?
-                                                                          
+++

Case of storing and accessing GRIB files on HDFS
-       
Since the size of GRIB files for a specific time period is quite large, it may surpass the size of a local disk.
In order to handle them, you may want to store them on HDFS. The case of accessing the weather data files on HDFS 
requires a different format from GRIB (**.grb2**). Specifically, GRIB files should be converted to **.nc** files. 
Currently, accessing GRIB files from HDFS is not supported by the library +++. Note that the size an '.nc' file
is quite larger than the size of its corresponding GRIB file.

You can convert a GRIB file to an nc file +++


Getting started
-                                               
Having downloaded the GRIB files, you are almost a step before starting using the weather data integrator. 
All that is left, is cloning the repository and executing some lifecycle stages in Maven. 

```
$ git clone https://github.com/nkoutroumanis/Weather-Integrator
$ cd  Weather-Integrator/
$ mvn install
```

### Weather Integrator out of the box

In case you want to use the weather integrator instantly, then you have to continue with a few commands;

```
$ cd weather-integrator-app/
$ mvn clean compile assembly:single 
$ cd target/
$ cp ../../weather-integrator-app/src/main/resources/reference-WeatherIntegrator.conf ./application.conf
```

Now, you can set the parameters about the integration procedure such as input files directory and etc.
with the following command. You can also open it with your favourite text editor.

```
$ sudo nano application.conf
```  

Then, you are ready to start the integration procedure with the following command;
```
$ java -cp weather-integrator-app-1.0-SNAPSHOT-jar-with-dependencies.jar gr.ds.unipi.wi.WeatherIntegratorJob ./application.conf
```  
              
### Weather Integrator API usage

If you want to run the integration procedure by using its API, then you have at first to add the following to your pom dependencies;

```xml
<dependency>
  <groupId>gr.ds.unipi</groupId>
  <artifactId>weather-integrator</artifactId>
   <version>1.0-SNAPSHOT</version>
</dependency>
```  

**Instantiate a Datasource object**

This object defines the datasource of the input spatio-temporal records.

The Datasource may be Files in a specific local path;
```java
// The first argument is the path of the files (dataset) that contain the records
// The second argument is the files extension such as .csv
Datasource datasource = FileDatasource.newFileDatasource("/path/folder/of/files", ".csv");
```

or may be a Kafka topic; 
```java
// The first argument is the path of the property file of the kafka consumer that will access the records from the topic
// The second argument is the name of the topic that contains the records
Datasource datasource = KafkaDatasource.newKafkaDatasource("/path/to/propertyFile/of/consumer", "topicName");
```

**Instantiate a RecordParser object**

This object defines the format type of the input spatio-temporal records.
The records may exist in a delimiter separated format type;
```java
// The first argument is the Datasource type object
// The second argument is the separator of the delimeter separated records
// The third argument is the field number of longitude (the first field in a row is represented as 1)
// The forth argument is the field number of latitude
// The fifth argument is the field number of date
// The sixth argument is the date format in Java which is represented as string. This field can also be 'unixTimestamp'
RecordParser recordParser = new CsvRecordParser(datasource, ";", 1, 2, 3, "yyyy-MM-dd HH:mm:ss");

```
or may exist in a JSON format type;
```java
// The first argument is the Datasource type object
// The second argument is the field name of the longitude
// The third argument is the field name of the latitude
// The forth argument is the field name of the date
// The sixth argument is the date format in Java which is represented as a string. This field can also be 'unixTimestamp'
RecordParser recordParser = new JsonRecordParser(datasource, "longitudeFieldName", "latitudeFieldName", "dateFieldName", "yyyy-MM-dd HH:mm:ss");
```

**Instantiate an Output object**

This object defines output source of the enriched spatio-temporal records.
The enriched records may be written to Files;
```java
// The first argument is the path of in which the output (enriched) records will be stored
// The second argument is the boolean value which determines if the declared output path should be deleted (if exist) before starting to write the enriched records 
Output output = FileOutput.newFileOutput("/path/of/output/", false);
```
or to a Kafka Topic;
```java
// The first argument is the property file path of the kafka producer
// The second argument is the name of the topic in which the enriched records will be produced
Output output = KafkaOutput.newKafkaOutput("/path/of/output", "topicName");
```

**Instantiate a WeatherIntegrator object**

This object can trigger the data integration procedure.

```java
// The first argument is the RecordParser type object
// The second argument is the path where the GRIB files are stored. It can also be an HDFS path, starting hdfs://.../.../
// The third argument is a list with the weather attributes which will be integrated to the records

WeatherIntegrator weatherIntegrator = WeatherIntegrator.newWeatherIntegrator(recordParser, "/path/to/grib/files/folder", List.of("weatherattr1", "weatherattr1"))
        .useIndex().build();

weatherIntegrator.integrate(output);
```

The builder pattern of the WeatherIntegrator object has the following methods;
* useIndex -  it is highly recommended to use this method. An index of the NetCDF library will be used for boosting the accessing of the weather data files.
* lruCacheMaxEntries - it accepts as an argument an integer which is the number of the cache entries. The higher the number, the higher the throughtput of the enriched records (if temporally unsorted). If the spatio-temporal records are already sorted by date, then set this 1. If not set, the default passed int argument is 4. 
* gribFilesExtension - it accepts as an argument a string which is the extension of the grib files. If not set, the default passed string argument is '.grb2'.
* removeLastValueFromRecords - if used, for each record that is to be enriched, the last value of every input record will not be included to the enriched record.
* filter - if used, only the spatio-temporal records that are enclosed by the (spatial) filter (Rectangle object argument) will taken into account for the enrichment procedure. You can create a Rectangle object with the following command; 

        // all of the arguments are double
        // longitude1 and latitude1 are the coordinates of the rectangle's lower bound
        // longitude2 and latitude2 are the coordinates of the rectangle's upper bound

        Rectangle.newRectangle(longitude1, latitude1, longitude2, latitude2)

Further reading
-        
If you are interested to see more details about the weather integrator mechanism or any other information, 
please refer to [this](http://ceur-ws.org/Vol-2322/BMDA_1.pdf) paper which was presented at the 2nd 
International Workshop on "Big Mobility Data Analytics" (EDBT/ICDT Workshops 2019) on March 26, 2019 at Lisbon, Portugal.
