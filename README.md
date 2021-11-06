# Cloud Data warehouse

### Introduction

Music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in AWS S3 including user activity logs and song metadata in JSON format.
    
### Business requirements


- Analytics team want to find insights on what songs their users are listening to using dimension tables
- analytics team want dimension model optimized for queries on song play analysis in data warehouse 

### Success criteria

* Launch a AWS redshift cluster 
  * Redshift cluster able to access s3
* Create dimension model and ETL pipeline to prepare data for analytics team
  * Explore and load JSON files in S3 to Redshift staging tables
  * Define and create FACT and DIMENSION tables for a star schema model for data analytics purposes
* Create ETL pipeline to load data from staging tables to data warehouse tables on Redshift
* Able to Connect to the Redshift cluster and complete testing utilizing test queries
  
