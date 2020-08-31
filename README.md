# kafka-stream-analyzer
Aggregation events by time and hotel and hotel location

### Pre-Requisites
* Java 8+
* Scala 2.11+
* Spark 2.4+
* Maven 3.6

#### Project Details
* src/main/scala - Contains Scala source code
* src/test/scala - Contains Scala test source code  
* scripts - Contains support shell script to build and run the project


#### Build
Executes the unit test case and build the jar 
```shell script
./scripts/build.sh
```
at 
`target/kafka-stream-analyzer-1.0.jar`


#### Run
Run the Spark submit command as follows

Program arguments
1. kafka broker list
2. Hotel data topic name
3. Hotel location topic name (ActionCountByHotelLocation)

Main Classes :
* ActionCountByHotel  - Triggers streaming job to aggregate Hotel data
```shell script
./bin/spark-submit --class "com.analytics.entry.ActionCountByHotel" \
  --master local[*] /jar-path/kafka-stream-analyzer-1.0.jar localhost:9092 data
```
* ActionCountByHotelLocation  - Triggers streaming job to join aggregate Hotel data by its locations
```shell script
./bin/spark-submit --class "com.analytics.entry.ActionCountByHotelLocation" \
  --master local[*] /jar-path/kafka-stream-analyzer-1.0.jar localhost:9092 data locations
```


