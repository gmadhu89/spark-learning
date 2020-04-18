### Spark Session Vs Spark Context

## Spark Session subsumes Spark Context, SQL Context, Hive Context

## show() - default is 20 rows

## London Crime Data Analysis

from pyspark.sql import SparkSession

## Create a new session or use an existing session
spark = SparkSession.builder\
                    .appName("Analyzing London crime data")\
                    .getOrCreate()
                    



data=spark.read\
          .format("csv")\
          .option("header","true")\
          .load("C:/Users/mganesan.CCCIS/Desktop/NGA/Spark Learning/data/london_crime_by_lsoa.csv")
          

##data -> DataFrame

data.printSchema()
data.count()

data.limit(5).show()

data.dropna()  ##Drop Not Available or missing data , Read more on pandas to understand this

data=data.drop("lsoa_code") ## Dropping columns that I will not be using in the analysis
data.show()

##See distinct columns on the Dataframe

total_boroughs = data.select("borough")\
                      .distinct()

total_boroughs.show()                     
total_boroughs.count()

## Filter data on a particular column

hackney_data = data.filter(data['borough']=="Hackney")
hackney_data.show()


data_2015_2016 = data.filter(data['year'].isin(["2015","2016"]))

data_2015_2016.sample(fraction=0.1).show()  ## Sample function display a certain fraction of the dataset


data_2014_onwards = data.filter(data['year'] >= '2014')

data_2014_onwards.sample(fraction=0.1).show()

################### Aggregations and Groupings ##################################3

### Having issue accesing file from Server to Local

borough_crime_count = data.groupBy('borough')\
                          .count()
borough_crime_count.show()

borough_conviction_sum = data.groupBy('borough')\
                             .agg({"value":"sum"})\
                             .withColumnRenamed("sum(value)","convictions")


total_borough_convictions = borough_conviction_sum.agg({"convictions":"sum"})
total_borough_convictions.show()

total_convictions = total_borough_convictions.collect()[0][0]

## Finding Percentage Contribution of COnvictions on a per borough basis
import pyspark.sql.functions as func
borough_percentage_contribution = borough_conviction_sum.withColumn("% Contribution", func.round(borough_conviction_sum.convictions/total_borough_convictions * 100,2))

borough_percentage_contribution.printSchema()

## To View results in descending order

borough_percentage_contribution.orderBy(borough_percentage_contribution[2].desc())\
                               .show(5)
                               
## To Show data distrubuted by months of a specific year and getting the total convictions per month

convictions_per_month = data.filter(data['year'] == 2014)\
                            .groupBy('month')\
                            .agg({"value":"sum"})\
                            .withColumnRenamed("sum(value)","convictions")
                            
total_convictions_monthly =  convictions_per_month.agg({"convictions":"sum"})\
                                                  .collect()[0][0]          ## Check why this is used

monthly_percentage_contribution = convictions_per_month.withColumn("Percent",func.round(convictions_per_month.convictions/total_convictions_monthly*100),2)
monthly_percentage_contribution.columns()

monthly_percentage_contribution.orderBy(monthly_percentage_contribution.Percent.desc()).show()

##Other built in Aggregations and Visualizing data using Matplotlib

## Get total number of crimes on a per Category Basis , and view the data in descending order
## Create a new dataframe with only the year column
## Get min/max of the year
## Get Basic Stats information (using describe)
## View data in matrix format

crimes_category =  data.groupBy('major_category')\
                       .agg({"value":"sum"})\
                       .withColumnRenamed("sum(value)","convictions")

crimes_category.orderBy(crimes_category.convictions.desc()).show(10)

year_df = data.select('year')

year_df.agg({"year":"max"}).show()
year_df.agg({"year":"min"}).show()

year_df.describe().show()

## Crosstab representation
data.crosstab('borough','major_category')\
    .select('borough_major_category','Burglary','Drugs','Fraud or Forgery','Robbery')\
    .show()
## Borough - Y-Axis
## Major-Category X axis

##MatplotLib representation
## pip install maptplotlib

get_ipython().magic('matplotlib inline')\
import matplotlib.pyplot as plt
plt.style.use('ggplot')

def describe_year(year):
       yearly_details = data.filter(data.year == year)\
                            .groupBy('borough')\
                            .agg({"value":"sum"})\
                            .withColumnRenamed("sum(value)","convictions")
       borough_list = [x[0] for x in yearly_details.toLocalIterator()]
       convictions_list = [x[1] for x in yearly_details.toLocalIterator()]
       
       plt.figure(figsize=(33,10))
       plt.bar(borough_list,convictions_list)
       
       plt.title('Crime for the year '+ year,fontsize=30)
       plt.xlabel('Boroughs',fontzise=30)
       plt.ylabel('Convictions',fontsize=30)
       
       plt.xticks(rotation=90,fontsize=30)
       plt.yticks(fontsize=30)
       plt.autoscale()
       plt.show()
       
describe_year('2014')


######################################################################################################################################################

######################  Broadcast Variables and Accumulators #######################3

##Broadcast - In-memory, Shared accross all tasks in the workers
              # COpied from one another, not just from the main variable (Peer to Peer Copying also)
              ## Applications : TRaining data in ML.
       
## Accumulator - Variables that follow communtative and associative properties
## Long, Double , Collections are some Accumulator Variable types


from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .appName("Analyzing Soccer Players")\
                    .getOrCreate()
                    
players = spark.read\
               .format("csv")\
               .option("header","true")\
               .load("../datasets/player.csv")
               
players.printSchema()

players.show(5)               

player_attributes = spark.read\
                         .format("csv")\
                         .option("header","true")\
                         .load("../datasets/player_attributes.csv")
                         
player_attributes.printSchema()                         

player_attributes.show(5)

players.count()
player_attributes.count()

player_attributes.select('player_api_id')\
                 .distinct()\
                 .count()
                 
players = players.drop('id','player_fifa_api_id')
players.columns


players = players.dropna()
player_attributes = player_attributes.dropna()

from pyspark.sql.functions import udf

year_attributes_udf = udf(lambda date: date.split('-')[0])

player_attributes = player_attributes.withColumn(
                    "year",
                    year_attributes_udf(player_attributes.date)
                    )


player_attributes = player_attributes.drop('date')

player_attributes.columns

########################## Find the best Striker for the soccer Team ##########################################
'''
1) Get only 2016 players data based on year
2) Get total count and distinct count of player_api_id's  in the resultant data frame
3) Aggreate and get the  Average of some Metrics
4) Give specific Weightage for each Metrics and then calculate grade per metric
5) Get the strikers who have grade > 70 and order the results in desceding order
6) Join with the Players dataframe and get the player name information also for the top strikers
'''

pa_2016 = player_attributes.filter(player_attributes.year == 2016)
pa_2016.count()

pa_2016.select(pa_2016.player_api_id)\   
       .distinct()\
       .count()
       
pa_striker_2016 = pa_2016.groupBy('player_api_id')\
                        .agg({
                              "finishing":"avg",
                              "shot_power":"avg"
                              "acceleration":"avg"
                             })\
                        .withColumnRenamed("avg(finishing)","finishing")\
                        .withColumnRenamed("avg(shot_power)","shot_power")\
                        .withColumnRenamed("avg(acceleration)","acceleration")
                        
pa_striker.count()

pa_striker.show(5)

weight_finishing = 1
weight_shot_power = 2
weight_acceleration = 1

total_weight = weight_finishing + weight_shot_power + weight_acceleration

strikers = pa_striker_2016.withColumn("striker_grade",
                                        (pa_striker_2016.finishing * weight_finishing + \
                                        pa_striker_2016.shot_power * weight_shot_power + \
                                        pa_striker_2016.acceleration * weight_acceleration)/total_weight)
                                        
                                        
strikers = strikers.drop('finishing','shot_power','acceleration')                                       

strikers = strikers.filter(strikers.striker_grade > 70)\
                           .sort(strikers.striker_grade.desc())
                           
strikers.show(10)

strikers.count()
players.count()

## Inner equality join between players and Strikers dataframes , result set will have 2 player_api_id columns 
striker_details = players.join(strikers, players.player_api_id == strikers.player_api_id)
striker_details.columns()

## Different syntax if the joining column has same name - this will automaticlally show that column only once

striker_details =  players.join(strikers, ['player_api_id'])

striker_details.show(5) ## Top 5 strikers

#####################################################################################################################

############### Using Broadcast dataframes for efficient Performance while joining ##############3
## Broadcast the dataframe that is smaller , since they have to be trasmitted to all nodes for processing

from pyspark.sql.functions import broadcast

striker_details = players.select(
                                  "player_api_id",
                                  "player_name")\
                         .join(
                                broadcast(strikers),
                                ["player_api_id"],
                                'inner'
                               )
                               
striker_details = striker_details.sort(striker_details.striker_grade.desc())

striker_details.show(5)
#####################################################################################################################

## Checking height of player on better accuracy ####

players.count()
player_attributes.count()

players_heading_acc = player_attributes.select('player_api_id','heading_accuracy')\
                                       .join(broadcast(players),
                                        player_attributes.player_api_id == players.player_api_id)
                                        
players_heading_acc.columns

## Setting up accumulators

short_count = spark.sparkContext.accumulator(0)
med_low_count = spark.sparkContext.accumulator(0)
med_high_count = spark.sparkContext.accumulator(0)
tall_count = spark.sparkContext.accumulator(0)


## Note, counts per row and not per player, there will be multiple players per node
def count_players_by_height(row):
    height = float(row.height)
    
    if (height <= 175 ):
        short_count.add(1)
    elif (height > 175 and height <= 183):
        med_low_count.add(1)
    elif (height > 183 and height <=195):
        med_high_count.add(1)
    elif (height > 195):
        tall_count.add(1)

players_heading_acc.foreach(lambda x : count_players_by_height(x))  ##processing distributed accross multiple nodes

all_players = [short_count.value,med_low_count.value,med_high_count.value,tall_count.value]

all_players

## Setup new accumulators to count the number of players who had met a specific heading accuracy

short_ha_count = spark.sparkContext.accumulator(0)
med_ha_low_count = spark.sparkContext.accumulator(0)
med_ha_high_count = spark.sparkContext.accumulator(0)
tall_ha_count = spark.sparkContext.accumulator(0)

def count_players_by_height_accuracy(row, threshold):
       height = float(row.height)
       ha = float(row.heading_accuracy)
       
       if ha <= threshold:
            return 
       
       if (height <= 175 ):
            short_ha_count.add(1)
       elif (height > 175 and height <= 183):
            med_ha_low_count.add(1)
       elif (height > 183 and height <=195):
            med_ha_high_count.add(1)
       elif (height > 195):
            tall_ha_count.add(1)

players_heading_acc.foreach(lambda x: count_players_by_height_accuracy(x,60))

all_players_above_threshold = [short_ha_count.value,med_ha_low_count.value,med_ha_high_count.value,tall_ha_count.value]
all_players_above_threshold


percentage_values = [
                       short_ha_count.value/short_count.value * 100,
                       med_ha_low_count.value/med_low_count.value * 100,
                       med_ha_high_count.value/med_high_count.value * 100,
                       tall_ha_count.value / tall_count.value * 100]
                       
                       
percentage_values

############################################################################################################################
## Save Dataframes to CSV and JSON FIles 

pa_2016.columns   ## we will save 2 fileds from this Dataframe into a CSV/JSON file


## To Save as CSV
pa_2016.select("player_api_id","overall_rating")\
       .coalesce(1)\            ## Combines data from all partitions to a single partition
       .write\
       .option("header","true")\
       .csv("players_overall.csv")
       
## To Save as JSON
pa_2016.select("player_api_id","overall_rating")\
       .write\
       .json("players_overall.json")
       
##Files will be saved to Current working directory

#################   CUSTOM ACCUMULATORS #############################################################
## Custom Vector Accumulator

## Reference the Accumulator PAram library to build custom accumulators
from pyspark.accumulators import AccumulatorParam

class VectorAccumulatorParam(AccumulatorParam):
        def zero(self,value):
               return [0.0] * len(value)
        
        def addInPlace(self, v1, v2):
                for i in range(len(v1)):
                        v1[i] += v2[i]
                        
                return v1
                
                
vector_accum = sc.accumulator([10.0,20.0,30.0],VectorAccumulatorParam())
vector_accum.value

vector_accum += [1,2,3]
vector_accum.value

######################################################################################################
## Types of Join Operators in Spark
## Inner, Left, Right , Full

valuesA = [('John',100000),('Emily',650000),('James',150000),('Nina',200000)]
tableA = sc.createDataFrame(valuesA, ['name','salary'])
tableA.show()

valuesB = [('John',1),('Emily',2),('Bob',3),('Kevin',4)]
tableB = sc.createDataFrame(valuesA, ['name','Eid'])
tableB.show()

inner_join = tableA.join(tableB, tableA.name == tableB.name)
inner_join.show()

left_join = tableA.join(tableB, tableA.name == tableB.name, how = 'left')
left_join.show()


right_join = tableA.join(tableB, tableA.name == tableB.name, how = 'right')
right_join.show()


full_join = tableA.join(tableB, tableA.name == tableB.name, how = 'full')
full_join.show()

######################################################################################################