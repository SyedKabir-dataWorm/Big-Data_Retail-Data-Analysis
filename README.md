----------------------------------------------------------------------------------------
Background
----------------------------------------------------------------------------------------

Historical sales data for 45 stores of a company are provided located in different regions
- each store contains a number of departments. The company also runs several promotional
markdown events throughout the year. These markdowns precede prominent holidays, the
four largest of which are the Super Bowl, Labor Day, Thanksgiving, and Christmas. The
weeks including these holidays are weighted five times higher in the evaluation than
non-holiday weeks.
----------------------------------------------------------------------------------------
DATA DESCRIPTIUON : Corresponding file in the repository: data
----------------------------------------------------------------------------------------
Data-Set:

- Three datasets:
- Features, sales, and stores dataset

Meta-data:

----------------Feature CSV----------------------------

Store: the store number
Date : Date of the week
Temperature:  average temperature in the region
Fuel_Price:  cost of fuel in the region
MarkDown1-5:  anonymized data related to promotional markdowns. MarkDown
data is only available after Nov 2011, and is not available for all stores all the
time. Any missing value is marked with an NA
CPI: the consumer price index
Unemployment: the unemployment rate
IsHoliday: whether the week is a special holiday week

-------------------Sales CSV----------------------------
Store: the store number
Dept: the department number
Date: Date of the week
Weekly_Sales: sales for the given department in the given store
IsHoliday: whether the week is a special holiday week

-------------------Stores CSV-----------------------------
Store: the store number
Type: the store type
Size: the size of the

--------------------------------------------------------------------------------------
TASKS
--------------------------------------------------------------------------------------

----------------------------Part 1: Working with RDDs (30%)---------------------------

In this section, you will need to create RDDs from the given datasets, perform partitioning in
these RDDs and use various RDD operations to answer the queries for retail analysis.

1.1 Data Preparation and Loading (5%)
1. Write the code to create a SparkContext object using SparkSession, which tells Spark
how to access a cluster. To create a SparkSession you first need to build a SparkConf
object that contains information about your application, using Melbourne time as the
session timezone. Give an appropriate name for your application and run Spark
locally with as many working processors as logical cores on your machine.
2. Load the features, sales and stores csv file into features, sales and stores RDDs.
3. For each features, sales and stores RDDs, remove the header rows and display
the total count and first 10 records. Hint : You can use csv.reader to parse rows in
RDDs.

1.2 Data Partitioning in RDD (15%)
1. How many partitions do the above RDDs have? How is the data in these RDDs
partitioned by default, when we do not explicitly specify any partitioning strategy? Can
you explain why it will be partitioned in this number? If I only have one single core CPU
in my PC, what is the default partition's number? Hint: search the source code to try to
answer this question.
2. Create a key value RDD for the store RDD, use the store type as the key and all of the
columns as the value. Print out the first 5 records of the key-value RDD.
3. Write the code to seperate the store key-value RDD based on the store type (the same
type should be in the same partition). Print out the total partition's number and the number of
records in each partition.

1.3 Query/Analysis (10%)
For this part, write relevant RDD operations to answer the following questions.
1. Calculate the average weekly sales for each year.
2. Find the highest temperature record in 2011 in the 'type B' store. You should display the
store ID, date, highest temperature and type in the result.

--------------------------------Part2. Working with DataFrames (50%)--------------------------
In this section, you will need to load the given datasets into PySpark DataFrames
and use DataFrame functions to answer the queries.

2.1 Data Preparation and Loading (5%)
1. Load features, sales and stores datasets into three separate dataframes. When you create
your dataframes, please refer to the metadata file and think about the appropriate data type
for each columns (Note: you should read the date column as the string type)
2. Display the schema of the features, sales and stores dataframes.

2.2 Query/Analysis (45%)
Implement the following queries using dataframes.You need to be able to perform operations
like filtering, sorting, joining and group by using the functions provided by the DataFrame API.
1. Transform 'Date' column in both features and sales dataframe to the date type,
After that print out these two DFs schema to show the results.
2. Calculate the average weekly sales for holiday week and non-holiday week separately,
order your result based on the average weekly sales in descending order. Print out the
IsHoliday and average sales columns.
3. Based on different years and months, calculate the average weekly sales
4. Calculate the average ‘MarkDown1’ value in holiday week for all type C stores.
5. Show all stores total sales based on each different month and yearly total in 2011
(since we have full 2011 data for every store) for every different store, only keep two
decimal places after the decimal point.
6. Draw a scatter plot to show the relationship between weekly sales and unemployment
rate, use the different colors for the holiday week point. After that, discuss your
findings based on the scatter plot.

--------------------------------Part3: RDDs vs DataFrame vs Spark SQL (20%)-------------------
Implement the following queries using RDDs, DataFrames and SparkSQL separately. Log
the time taken for each query in each approach using the “%%time” built-in magic
command in Jupyter Notebook and discuss the performance difference between these 3
approaches.

Query: Calculate the average weekly fuel price for all stores' size larger than 150000.

Also talk about this question in a detailed answer, “Why is DF faster than RDD?”
