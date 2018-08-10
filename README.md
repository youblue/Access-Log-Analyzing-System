# Get Top Shopping Hour in US

### Run on HDFS:
```
1. Go to the project dir
cd ${PROJECT_DIR}/topShoppingHour

2. Upload data to HDFS
hadoop fs -put ./data/access_log_sample .

3. Make sure output does not exist
hadoop fs -rm -r output

4. Run Spark
spark-submit --master spark://ea63ad7cb04b:7077 --class spark.GetTopShoppingHour --jars resource/geoip-api-1.3.1.jar --files resource/GeoLiteCity.dat target/java-1.0-SNAPSHOT.jar

5. Check output folder:
hadoop fs -ls output

6. Merge all output files and check result:
hadoop fs -getmerge output output.txt
cat output.txt

# The result is:
15,11935
16,8581
14,8083
17,2788
2,2400
1,2394
0,2373
23,2290
3,2122
21,2113
19,2075
18,2071
20,2061
22,2034
4,1508
13,1089
5,1003
12,733
6,625
11,452
7,422
8,267
9,252
10,247
```

### Run on AWS:

```
1. Upload the following file into S3:
java-1.0-SNAPSHOT.jar
geoip-api-1.3.1.jar
GeoLiteCity.dat

2. Spark submit options:
--class spark.GetTopShoppingHour --jars s3://bittiger-cs502-zws/geoip-api-1.3.1.jar --files s3://bittiger-cs502-zws/GeoLiteCity.dat

3. Application location:
s3://bittiger-cs502-zws/java-1.0-SNAPSHOT.jar

4. Check output result:
In "s3://bittiger-cs502-zws/sparkOutput", downloaded as:
"part-00000-2bcf1fce-1db6-46ed-b9ed-a9d0b58fb14d-c000.csv"

```
