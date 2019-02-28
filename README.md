# spark demo

## 0: Build the new spark connector for 2.3

 - git clone https://github.com/couchbase/couchbase-spark-connector.git
 - cd couchbase-spark-connector
 - ./gradlew publishToMavenLocal

## 1: load cars dataset into cars bucket


https://drive.google.com/file/d/1-xuXlAO1mQBBB3ihiRIWTtRcKZ97NT94/view?usp=sharing

```
/opt/couchbase/bin/cbbackupmgr restore --archive /home/daschl/Downloads/data/backup/bakcup --repo cars -c localhost -u Administrator -p password
```

## 2: load the locations data into locations bucket

https://s3.amazonaws.com/data.openaddresses.io/openaddr-collected-us_west.zip

(importing only two parts of california due to size, feel free to load more)

```
/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/bin/cbimport csv -c localhost:8091 -u Administrator -p password -b locations -d file:///Users/daschl/Downloads/openaddr-collected-us_west/us/ca/san_bernardino.csv -g %HASH%
CSV `file:///Users/daschl/Downloads/openaddr-collected-us_west/us/ca/san_bernardino.csv` imported to `http://localhost:8091` successfully, 820699 documents loaded
```

```
/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/bin/cbimport csv -c localhost:8091 -u Administrator -p password -b locations -d file:///Users/daschl/Downloads/openaddr-collected-us_west/us/ca/city_of_mountain_view.csv -g %HASH%
CSV `file:///Users/daschl/Downloads/openaddr-collected-us_west/us/ca/city_of_mountain_view.csv` imported to `http://localhost:8091` successfully, 38654 documents loaded
```

## 3: add datasets for analytics
 - create dataset cars on cars;
 - create dataset locations on locations;
 - connect link Local;
 
## 4: Run the main App in Intellij

It runs a query to fetch all cars that have locations and then runs a k-means clustering algorithm with engine temp, outside temp and speed to build the model. Then it runs all cars again through the algo to "predict" their group (you could ofc train it with less and let it infer). I've only printed a groupby since those are lots of cars.. one could plot this onto a graph and do more elaborate clustering, but it shows the point of how it can work.

It takes a little bit to run but you should see the number of car data grouped into a cluster and then the cluster centers:

```
+---------+--------+
|predicted|count(1)|
+---------+--------+
|        0|   10243|
|        7|    9043|
|        6|    7320|
|        9|    9419|
|        5|    6286|
|        1|    8610|
|        3|    8757|
|        8|    7730|
|        2|    7484|
|        4|    7243|
+---------+--------+

[EngineTemp, OutsideTemp, Speed]
0: [197.28835849975644,66.9486604968339,19.813541159279104]
1: [199.24701518488467,97.27564622696187,72.37892662570998]
2: [201.5801342281879,38.44885906040268,79.76053691275168]
3: [200.55131128848348,95.86088939566704,12.857126567844926]
4: [203.24301015440255,39.76589233551259,36.00945889553484]
5: [192.0565237870937,42.211807191081796,56.597739048516246]
6: [198.65876842540294,70.25292740046838,83.35183909629426]
7: [204.6075642297938,68.30488477230125,53.40599845627963]
8: [200.92986572806674,39.5828444792074,10.843697040803024]
9: [199.96851890978238,95.19004859497147,42.18054088316079]
```