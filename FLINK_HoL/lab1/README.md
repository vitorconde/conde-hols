# Fraud Detection with Apache Kudu and SQL Stream Builder

**Goals**

- [ ] Common understanding of Sql Stream Builder UI
- [ ] Complete Necessary Project Setups
- [ ] Execute SQL On Kafka Topics
- [ ] Buil a complicated Fraud Detection Sql Stream Builder Job

Before we get started with Kudu and Sql Stream Builder switch over to your Streams Messaging Manager UI.  

![01 High Level Architecture](../../assets/images/01_High_Level_Architecture.png)

## Apache Kafka

In Topics Search box type `txn` and confirm you see the `txn1` and `txn2` topics:

![24 SMM Kafka Topic]()
<img src="../../assets/images/24_SMM_Kafka_Topics.png" width="85" align="absmiddle">


Select those two topics so we see them center screen like this:

![25 SMM Kafka Topic](../../assets/images/25_SMM_Kafka_Topics.png)

Now check we have messages coming in Apache Kafka. To view the data click <img src="../../assets/images/26_SMM_Kafka_Topics_View.png" width="85" align="absmiddle"> you see to the right.

Next click this button: <img src="../../assets/images/26_SMM_Kafka_Topics.png" width="85" align="absmiddle"> and on the next topic page, take the Data Explorer tab to see transactions:

![09 Streams Messaging Manager](../../assets/images/09_Streams_Messaging_Manager.png)

## Hue Database Setup

Switch to your HUE UI.  Before you start to query, notice the Hue UI variable `user_id` at the bottom of the query window.  Enter your unique userXXX like this:

![20 Hue User Id](../../assets/images/20_Hue_Userid.png)


Now execute the following statements:

``` javascript
CREATE DATABASE ${user_id}_fraud;
CREATE TABLE ${user_id}_fraud.fraudulent_txn_kudu
(
event_time string,
acc_id string,
transaction_id string,
f_name string,
l_name string,
email string,
gender string,
phone string,
card string,
lat double,
lon double,
amount bigint,
PRIMARY KEY (event_time, acc_id)
)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES ('kudu.num_tablet_replicas' = '3');
```

This will create your `users` database and `fraudulent_txn_kudu` kudu table for Fraudulent Transactions.


## Sql Stream Builder

Switch to your Streaming SQL Console.   Below is an example of what your finished SSB Project will look like.   Breakout room leader should have a live copy ready to show attendees around.

 ![09.5 Intro to SSB](../../assets/images/09.5_Intro_SSB.png)

Be sure to have a look around the UI.  Notice all the menus, and sub menus.

Inspect the Left Navigation and notice the Hover Names:

![21 SSB Left Nav](../../assets/images/21_SSB_Left_Nav.png)

Fully open the Explorer Tab and notice different right menus depending on the entity:

![22 New Kafka Table](../../assets/images/22_New_Kafka_Table.png)
![23 New Catalog](../../assets/images/23_New_Catalog.png)

The Sql Stream Builder UI is deep and wide.  Take your time to explore all of the different areas throughout your time in this hands on lab.


### Create and Activate an Environment Variable 

  First, click left nav "Environments". Create and environment and create a key value pair for your userid -> username. 

 ![09.6 SSB Environment Variable](../../assets/images/09.6_SSB_Environment_Variable.png)

!!! warning
     Make sure you click Add to add the key value pair, then click Create. 

 ![09.7 SSB Environment Variable](../../assets/images/09.6_SSB_Environment_Activate.png)

!!! danger
     Be sure to activate after creation. 

### **Setting Up Data Sources**

Next we need to set up the Data Sources and Data Catalogs from Streaming SQL Console.  Follow screen shots below, be sure to Validate each before clicking Create.


First, add a New Kafka Data Source

<img src="../../assets/images/M0_91_Add_Kafka_Cluster.png" width="450">

Second, add a new Catalog for the Schema Registry

<img src="../../assets/images/M0_92_Add_Schema_Registry.png" width="450">

Third, add a new Catalog for the Kudu Catalog.

<img src="../../assets/images/M0_93_Add_Kudu_Catalog.png" width="450">

Last, add a new Catalog for the Hive Catalog.

<img src="../../assets/images/M0_94_Add_Hive_Catalog.png" width="450">


### **Setting Up Virtual Tables**

To start using SSB, we need to create some virtual tables. In SSB, a Virtual Table is a logical definition of the data source that includes the location and connection parameters, a schema, and any required, context for specific configuration parameters. Tables can be used for both reading and writing data in most cases. You can create and manage tables either manually or they can be automatically loaded from one of the catalogs as specified using the Data Providers section(2).

A table defines the schema of events in a Kafka topic. For instance, we need to create 2 tables txn1 and txn2. SSB provides an easy way to create a table :

![11 Create SSB Kafka Table 1](../../assets/images/11_Create_SSB_Kafka_Table_1.png)

Make sure that you are using the Kafka timestamps and rename the "Event Time Column" to event_time

![12 Create SSB Kafka Table 2](../../assets/images/12_Create_SSB_Kafka_Table_2.png)

This creates a table called txn1 that points to events inside the txn1 Kafka topic. These events are in JSON format. It also defines an event_time field which is computed from the Apache Kafka Timestamps and defines a watermark of 3 seconds. Similarly, we need to create a txn2 table before using them in SSB.

We are ready to query our tables.  Create a new Job in SSB with the following SQL query: 

``` javascript
SELECT * FROM txn1;
```
!!! danger
     Be sure to adjust the txn1 virtual table to match the virtual table(s) you created.  

Querying streaming data is now as easy as querying data in a SQL database. Here’s how this looks like in the SSB console. Events are continuously consumed from Apache Kafka and printed in the UI:

![13 SSB Simple Select Query](../../assets/images/13_SSB_Simple_Select_Query.png)

### **Stream to Stream Joins**

Remember, the objective here is to detect fraudulent transactions matching the following pattern, we will consider two transactions with the same "account_id" :

- Occurring in 2 different locations,
- With a distance greater than 1 KM,
- And with less than 10 minutes between them.

To do so, let’s first join the txn1 and txn2 streams on attribute transaction_id:

``` javascript
SELECT
txn1.ts as EVENT_TIME,
txn2.ts,
txn1.account_id as ACCOUNT_ID,
txn1.transaction_id AS TRANSACTION_ID,
txn2.transaction_id,
txn1.amount as AMOUNT,
txn1.lat AS LAT,
txn1.lon AS LON
FROM txn1
INNER JOIN txn2
on txn1.account_id=txn2.account_id
```
!!! warning
     You may need to use autocomplete in the query above to fix the FROM txn1 INNER JOIN txn2 virtual tables here.  

The output from SSB console:

![14 Stream To stream Joins](../../assets/images/14_Stream_To_stream_Joins.png)

Now, we need to filter out :

- The events with the same location,
- The same events that match to self,
- With a distance between 2 locations less than 1KM,
- Within an interval of 10 minutes,
- Remember, the fraudulent transactions have a prefix of 'xxx'.

With SSB, we can create user functions (UDFs) to write functions in Python. Since, there is no out-of-the box function in SSB to calculate the distance between 2 locations, let’s use the UDF feature in order to enhance the functionality of our query. More details on UDF are available [here](https://docs.cloudera.com/cdf-datahub/7.3.1/how-to-ssb/topics/csa-ssb-add-python-udf.html).

The Python function will use the [Haversine_formula](https://en.wikipedia.org/wiki/Haversine_formula).

``` javascript
from pyflink.table.udf import udf
from pyflink.table import DataTypes
import math

@udf(result_type=DataTypes.FLOAT())
def udf_function(lat1,lon1,lat2,lon2):
    def toRad(x):
        return float(x) * math.pi / 180

    R = 6371 # km
    x1 = lat2 - lat1
    dLat = toRad(x1)
    x2 = lon2 - lon1
    dLon = toRad(x2)
    a = (math.sin(dLat / 2) * math.sin(dLat / 2) +
      math.cos(toRad(lat1)) * math.cos(toRad(lat2)) *
      math.sin(dLon / 2) * math.sin(dLon / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = R * c

    return d
```

From SSB Console :

![15 SSB User Defined Function UDF test](../../assets/images/15_SSB_User_Defined_Function_UDF.png)

Now, let’s run our query that implements our pattern :

``` javascript
SELECT
      txn1.ts as EVENT_TIME,
      txn2.ts,
      txn1.account_id as ACCOUNT_ID,
      txn1.transaction_id AS TRANSACTION_ID,
      txn2.transaction_id,
      txn1.amount as AMOUNT,
      txn1.lat AS LAT,
      txn1.lon AS LON,
      HAVETOKM(txn1.lat,txn1.lon,txn2.lat,txn2.lon) as distance

FROM txn1
INNER JOIN txn2
      on txn1.account_id=txn2.account_id
where
      txn1.transaction_id <> txn2.transaction_id
      AND (txn1.lat <> txn2.lat OR txn1.lon <> txn2.lon)
      AND txn1.ts < txn2.ts
      AND HAVETOKM(txn1.lat,txn1.lon,txn2.lat,txn2.lon) > 1
      AND txn2.event_time  BETWEEN txn1.event_time - INTERVAL '10' MINUTE AND txn1.event_time
```
!!! warning
     Make sure you use autocomplete in the query above to fix the FROM txn1 INNER JOIN txn2 virtual tables here.  

![16 SSB Stream To Stream Joins Filter Out](../../assets/images/16_SSB_Stream_To_Stream_Joins_Filter_Out.png)

### **Stream to Stream Joins and Enrichment**

In the previous paragraph, we have taken an inbound stream of events and used SSB to detect transactions that look potentially fraudulent. However, we only have account_id, transaction_id and location attributes. Not really useful. We can enrich these transactions by joining the previous results with some metadata information like username, firstname,address,phone from the "customer" Apache Kudu table. We will write back the results in another Kudu table called "fraudulent_txn_kudu".

Now, let's build the final Insert Query.  Be sure to use auto complete to find your fraudulent_txn_kudu table.

``` javascript
INSERT INTO fraudulent_txn_kudu
SELECT EVENT_TIME, ACCOUNT_ID, TRANSACTION_ID, cus.first_name as FIRST_NAME ,cus.last_name as LAST_NAME,cus.email as EMAIL ,cus.gender as GENDER, cus.phone as PHONE , cus.card as CARD , LAT, LON, AMOUNT
FROM (
SELECT
txn1.ts as EVENT_TIME,
txn2.ts,
txn1.account_id as ACCOUNT_ID,
txn1.transaction_id AS TRANSACTION_ID,
txn2.transaction_id,
txn1.amount as AMOUNT,
txn1.lat AS LAT,
txn1.lon AS LON,
HAVETOKM(txn1.lat,txn1.lon,txn2.lat,txn2.lon) as distance
FROM txn1
INNER JOIN txn2
on txn1.account_id=txn2.account_id
where
txn1.transaction_id <> txn2.transaction_id
AND (txn1.lat <> txn2.lat OR txn1.lon <> txn2.lon)
AND txn1.ts < txn2.ts
AND HAVETOKM(txn1.lat,txn1.lon,txn2.lat,txn2.lon) > 1
AND txn2.event_time BETWEEN txn1.event_time - INTERVAL '10' MINUTE AND txn1.event_time
) FRAUD
JOIN `Kudu`.`default`.`default.customers` cus
ON cus.account_id = FRAUD.ACCOUNT_ID
```
!!! warning
     You may need to use autocomplete in the query above to fix the INSERT INTO fraudulent_txn_kudu and FROM txn1 INNER JOIN txn2 virtual tables here.   

Next, notice the Settings for this job top right next to Job Actions.  Open the Job Settings so that we can enable checkpointing.  Enabling checkpointing during testing ensure the results we want will persist and write to Kudu.

In job settings, enable checkpointing as follows:

<img src="../../assets/images/M2_01_Job_Settings.png" width="450">

Save and Execute your job.  Be patient waiting for results to begin polling.

Now we can see from the output that all the fraudulent transactions are displayed in the SSB console:

![18 Stream To Stream Enrich](../../assets/images/18_Stream_To_Stream_Enrich.png)

From Hue, we can see that the results are written to the Apache Kudu Table :

![19 Stream To Stream Hue View Kudu Table](../../assets/images/19_Stream_To_Stream_Hue_View_Kudu_Table.png)

### Checklist

- [ ] Were you able to get into your correct UserId SSB Project?
- [ ] Complete setup of Data Sources for SSB Project?
- [ ] Execute SQL on your Kafka Topics?
- [ ] Complete insert of Fraud Transactions into Kudu and validated results arrived in Hue?

**:rocket: We have now concluded Lab 1 :rocket:**
