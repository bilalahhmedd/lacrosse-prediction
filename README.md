# Project Title

Lacrosse-Predicition

## Description

This is data engineering repo for lacrosse-prediciton project. It contains end to end data ingestion system which includes Web Scrappers, ETL and Data Lake folders.
## Getting Started

### Dependencies

<<<<<<< HEAD
* linux operating system
=======
<<<<<<< HEAD
* linux operating system
=======
<<<<<<< HEAD
* linux operating system
=======
* linux operating system
>>>>>>> 193481523b926ce17850926936a84e5af9f6f936
>>>>>>> origin/dev
>>>>>>> c6755bc040793a9acfd04604f3acee653e22aa2d
* python >= 3.6
* pip3 latest
* salenium
* pandas
* xml 
* pylint
* sql alchemy
* google chrome browser
* google chrome driver
* chrome autoInstaller

### Installing

* clone repo to any linux based env i.e aws instance, gcp cloud machine, ubuntu desktop etc.
* navigate to project root folder using terminal
* run command `source setup.sh`
    * upgrade pip with latest version 
    * create directory '/home/user/envs/lacrosse-prediciton/'
    * create virtual env in above directory
    * activate env lacrosse_prediction
    * install dependencies using requirements.txt

### Executing program


#### bash jobs
* activate python environment
``` source /home/user-name/lacrosse_prediction/bin/activate```
* run scrapping job for given category i.e commitments 
``` bash cron_jobs/commitments_scrappers.sh ```
* run all scrappers
``` bash cron_jobs/run_scrappers_all.sh ```
* run etl for all new data
``` bash cron_jobs/run_etl_all.sh ```


#### py modules
* activate python environment
``` source /home/user-name/lacrosse_prediction/bin/activate```
* run any module individual
* Run scrapers example
``` python3 scrappers/commitments_laxnumbers.py```
* Run Etl example
``` python3 etl/etl_commitments.py ```
* run data loader example 
``` python3 etl/load_csv_into_db_table.py --param commitments ```
* create ddl to create table from csv file
``` python3 etl/create_db_table_from_csv.py --param csv_file_path```
