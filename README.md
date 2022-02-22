# TECHNICAL TEST : ANALYZE E-COMMERCE DATA

You were recently hired by an E-commerce company. Your mission is to provide insights on sales.

There are four datasets :
* *Products*: a list of available products.
* *Items*: a list of items.
* *Orders*: a list of customer orders on the website.
* *Customers*: a list of customers.

Centralize your projet and code in a Github repository and provide the url once the test is completed.

**To Dos**
1. Get the four datasets into Spark
2. Each day we want to compute summary statistics by customers every day (spending, orders etc.)
Create a Spark script to compute for a given day these summary statistics.
3. Run that script over the necessary period to inject historic data. Then, identify the top customers
4. How many customers are repeaters ?


	[orders count by customers](https://github.com/TeriRomain/SparkExample/blob/main/warehouse/business/repeaters/repeaters_count_orders/part-00000-f8ddbf43-0754-452f-9430-9887e67f0177-c000.csv)
	
	
	[link for repeaters customers information](https://github.com/TeriRomain/SparkExample/blob/main/warehouse/business/repeaters/repeaters/part-00000-6aa055dc-6fb5-42a3-8084-d9ecd498af6a-c000.csv)
	
	[repeater count](https://github.com/TeriRomain/SparkExample/blob/main/warehouse/business/repeaters/nb_repeaters/part-00000-2fe51cb1-a934-4466-9e17-73bb008a02b1-c000.csv)

6. Optionnal : If you want to show more skills you have, add anything you find usefull :
	- To automate this and make it run every day
	- To bring it in a "Infra-as-Code" way
	- To add real-time on anything you want
	- Anything you want to show
