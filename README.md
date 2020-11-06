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
You can do it in two ways; either by opening a GRIB file with a GRIB data viewer or by fetching the weather attributes with NetCDF library.

If you choose to download a GRIB data viewer, I would recommend using [Panoply](https://www.giss.nasa.gov/tools/panoply/download/). Of course, 
there are other GRIB data viewers that you can also use.
 
If you prefer to use code for fetching the weather attributes, refer to one of the `exportVariablesFromFile` methods of
[this](https://github.com/nkoutroumanis/Weather-Integrator/blob/master/weather-integrator/src/test/java/gr/ds/unipi/wi/ExportVariablesFromGribFileTest.java) class.
Just modify the single argument of `NetcdfFile.open()` with the local path of the GRIB file, and the argument of `new PrintWriter()` with the local path of the 
txt file in which the attributes will be exported.


Case of storing and accessing GRIB files on HDFS
-       
Since the size of GRIB files for a specific time period is quite large, it may surpass the size of a local disk.
In order to handle them, you may want to store them on HDFS. The case of accessing the weather data files on HDFS 
requires a different format from GRIB (**.grb2**). Specifically, GRIB files should be converted to **.nc** files. 
Currently, accessing GRIB files remotely is not supported by the NetCDF library. Note that the size of an '.nc' file
is quite larger than the size of its corresponding GRIB file.

You can convert a GRIB file to an nc file by using the NetCDF library. Download from [here](https://www.unidata.ucar.edu/downloads/netcdf-java/index.jsp)
the netcdfAll-Y.Y.Y.jar file and then run the following command;

```
$ java -Xmx512m -classpath netcdfAll-Y.Y.Y.jar ucar.nc2.dataset.NetcdfDataset -in /full/path/of/gribFile/file.grb2 -out /full/path/of/ncFile/file.nc
```

Refer to [this](https://github.com/nkoutroumanis/Weather-Integrator/blob/master/convertToNcFiles.sh) script if you want to convert a group of GRIBs to nc files.

Getting started
-                                               
Having downloaded the GRIB files, you may follow the steps below so as to start using the weather data integrator.

Since the Weather Integrator is a maven project which uses the [_Spatiotemporal Processing Interface_](https://github.com/nkoutroumanis/Spatiotemporal-Processing-Interface) dependency and the library of [_SciSpark_](https://scispark.jpl.nasa.gov/),
it is required to clone the repositories and install them to your local maven repository.

For the _Spatiotemporal Processing Interface_, you may execute the following commands;
```
$ git clone https://github.com/nkoutroumanis/Spatiotemporal-Processing-Interface
$ cd Spatiotemporal-Processing-Interface/
$ mvn install
```

For the _SciSpark_, it is required after cloning the library, to build the Jar file through _sbt_ (tested with Java 8. Newer Java versions may not be suitable, as errors may occur during the creation of the Jar). Then, you may proceed to the repository installation.
In case you have not installed _sbt_, refer to [this](https://www.scala-sbt.org/1.x/docs/Setup.html) link before continuing. All these can be done by executing the following commands;
```
$ git clone https://github.com/SciSpark/SciSpark
$ cd SciSpark/
$ sbt assembly
$ mvn install:install-file -Dfile=target/scala-2.11/SciSpark.jar -DgroupId=org.dia -DartifactId=scispark -Dversion=1 -Dpackaging=jar  
``` 
 
 If you have any problems concerning the building the SciSpark project, contact me to pass you directly the Jar file you need.
 
Now, open a new terminal window and clone the Weather Integrator repository;
```
$ git clone https://github.com/nkoutroumanis/Weather-Integrator
```

Execute some lifecycle stages in Maven for the Weather Integrator repository. 

```
$ cd  WeatherIntegrator/weather-integrator
$ mvn install
$ cd ../weather-integrator-app
$ mvn install
$ cd ../
```

### Weather Integrator out of the box

In case you want to use the weather integrator instantly, then you have to continue with a few more commands;

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
// The sixth argument is the date format in Java which is represented as string. This field can also be 'unixTimestampSec', 'unixTimestampMillis' or 'unixTimestampDecimals'
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

WeatherIntegrator weatherIntegrator = WeatherIntegrator.newWeatherIntegrator(recordParser, "/path/to/grib/files/folder", List.of("weatherattr1", "weatherattr2"))
        .useIndex().build();

weatherIntegrator.integrate(output);
```

The builder pattern of the WeatherIntegrator object has the following methods;
* useIndex -  it is highly recommended to use this method. An index of the NetCDF library will be used for boosting the accessing on the weather data files.
* lruCacheMaxEntries - it accepts as an argument an integer which is the number of the cache entries. The higher the number, the higher the throughtput of the enriched records (if temporally unsorted). If the spatio-temporal records are already sorted by date, then set this 1. If not set, the default passed int argument is 4. 
* gribFilesExtension - it accepts as an argument a string which is the extension of the grib files. If not set, the default passed string argument is '.grb2'.
* removeLastValueFromRecords - if used, the last value of every input record will not be included to the enriched output record.
* filter - if used, only the spatio-temporal records that are enclosed by the (spatial) filter (Rectangle object argument) will taken into account for the enrichment procedure. You can create a Rectangle object with the following command; 

        // all arguments are double type variables
        // longitude1 and latitude1 are the coordinates of the rectangle's lower bound
        // longitude2 and latitude2 are the coordinates of the rectangle's upper bound

        Rectangle.newRectangle(longitude1, latitude1, longitude2, latitude2)

Further reading
-        
If you are interested to see more details about the weather integrator mechanism or any other information, 
please refer to [this](http://ceur-ws.org/Vol-2322/BMDA_1.pdf) paper which was presented at the 2nd 
International Workshop on "Big Mobility Data Analytics" (EDBT/ICDT Workshops 2019) on March 26, 2019 at Lisbon, Portugal.

An extension of the weather integrator mechanism for distributed enrichment on streaming data, is presented in [this](https://link.springer.com/article/10.1007/s10707-020-00423-w) article of GeoInformatica (2020) journal.

Contributors
-
Nikolaos Koutroumanis, Christos Doulkeridis; Data Science Lab., University of Piraeus.

Acknowledgment
-
This work was partially supported by the European Union’s Horizon 2020 research and innovation programme under grant agreements No 687591 (DATACRON), No 780754 (Track & Know), No 779747 (BigDataStack) and No 777695 (MASTER).