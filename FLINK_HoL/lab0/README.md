# Getting Started

**Goals**

- [ ] First Access Cloudera Control Plane
- [ ] Complete Initial User Setup Tasks
- [ ] Access all Required UIs

## Warnings
 * Login with your Cloudera Login
 * VPN is required
 * Be careful copy/paste strings w/ trailing characters.  Use GitHub Copy Buttons.
 * If needed create unique userXXX edits to topics, tables, etc
 * In HUE, use ${userid} to reference your unique userXXX
 * In SQL Stream Builder, create your env userid key with value of your unique userXXX
 * In SQL Stream Builder be careful with sample jobs.  Comments to indicate Execute Selections individually.  Do not run entire sample jobs.
 * In SQL Stream Builder **STOP** jobs when you are finished with each one.
 * You may need to use auto complete to in SSB to get full and correct path to assets.

```
 `Kudu`.`default`.`user099_fraud.fraudulent_txn_kudu`
```

```
FROM user099_txn1 txn1
INNER JOIN user099_txn2 txn2
```


***

## Copy Paste Board

You will be frequently copy/pasting the following details.  To make sure you get the correct values, use the COPY button top right.

### Kafka Brokers
```
{{ kafka_brokers }}
```
### Schema Registry URL
```
{{ schema_registry_url }}
```

### Kudu Masters
```
{{ kudu_masters }}
```

***

## How to Login to Cloudera Public Cloud

[Link To Login]( {{ link_to_login }} )

After login you should see CDP Public Cloud Home Screen

<img src="../../assets/images/M0_1_CDP_Home_Screen.png" width="750">

From any CDP Public Cloud page you can click this icon <img src="../../assets/images/M0_2_CDP_Home_Tile.png" width="25" height="25" align="absmiddle"> (top left) to open the full Left Navigation

  <img src="../../assets/images/M0_3_CDP_Left_Navigation.png" width="150">

## Workload Password Setup

From Home Screen click your Username (bottom left) and navigate to your Profile to click Set Workload Password:

<img src="../../assets/images/M0_4_CDP_Workload_Password.png" width="650">


## How to find Data Hub UIs (Hue, Schema Registry, Streams Messaging Manager, Sql Stream Builder)

From Home Screen click on Data Hub Clusters and search "csp-hol"

![M0_5 CDP Data Hubs](../../assets/images/M0_5_CDP_Data_Hubs.png)

Click into the csp-hol-kudu Data Hub and open Hue:

![M0_8 CDP Flink](../../assets/images/M0_8_CDP_Kudu.png)

Click into csp-hol-kafka Data Hub and open Schema Registry and Streams Messaging Manager UIs:

![M0_6 CDP Kafka](../../assets/images/M0_6_CDP_Kafka.png)

Click into csp-hol-flink Data Hub and open Streaming SQL Console:

![M0_7 CDP Flink](../../assets/images/M0_7_CDP_Flink.png)

Once you open the UI, click the   <img src="../../assets/images/M0_7.5_SSB_Switch.png" width="85" align="absmiddle"> to open your UserId's Default Project.


## How to Unlock Keytab in SSB

When you first open your default project, you must unlock your keytab to allow SSB to act on your behalf when creating data sources, executing jobs, etc.  On first login you will be presented with the following prompt:

<img src="../../assets/images/M0_9_SSB_Workload_Password.png" width="300">

Click into provide your username and workload password as follows:

<img src="../../assets/images/M0_10_SSB_Workload_Password.png" width="450">

Click Unlock Keytab:

<img src="../../assets/images/M0_11_SSB_Workload_Password.png" width="450">

### Checklist

- [ ] Are you able to connect to the Cloudera Control Plane?
- [ ] Did you set your Workload Password?
- [ ] Are you able to navigate to and open all the required UIs?
- [ ] Were you able to access your SSB Project and Unlock your Keytab?

**:rocket: We have now concluded Lab 0 :rocket:**
