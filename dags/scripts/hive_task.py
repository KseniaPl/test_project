import logging
from pyspark.sql import SparkSession
from pyspark.sql import Row
from airflow.models import Variable
from os.path import abspath

def hive_task():
	logging.info("hive task started")
	warehouse_location = abspath('spark-warehouse')

	spark = SparkSession \
	    .builder \
	    .enableHiveSupport() \
	    .getOrCreate()
	logging.info("before sql")
	spark.sql("CREATE TABLE IF NOT EXISTS vac ( id INT,region STRING,city STRING,name STRING,employer STRING,title STRING,\
	createdAtD INT,createdAtM INT,createdAtY INT,spec STRING,salary_from INT,salary_to INT,exp STRING,currency STRING,\
	skillsstr STRING,mode STRING,schedule STRING,description STRING,lat STRING,long STRING,category STRING)\
		  ROW FORMAT DELIMITED \
	FIELDS TERMINATED BY ','")
	logging.info("before load")
	filepath = '/home/kseniya/airflow/dags/scripts/nn_all.csv'
	spark.sql(f"LOAD DATA LOCAL INPATH '{filepath}' INTO TABLE vac")
	logging.info("after load")
	spark.sql("SELECT * FROM vac").show()
	spark.sql("SELECT category, count(category) FROM vac  group by category order by category ").show()
	spark.sql("SELECT exp, count(exp) FROM vac  WHERE category ='web' group by exp order by exp").show()
	spark.sql("SELECT name, count(name) FROM vac  group by name order by count(name) desc").show()
	spark.sql("SELECT percentile(salary_to, 0.25) ,percentile(salary_to, 0.5), \
	percentile(salary_to, 0.75) FROM vac  WHERE category ='web' and salary_to >0 group by exp\
	 order by exp").show()
	return 
