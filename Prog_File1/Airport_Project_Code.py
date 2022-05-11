from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]")\
        .appName("Airport_Proj")\
        .config("spark.driver.bindAddress","localhost")\
        .config("spark.ui.port","4050")\
        .getOrCreate()

    schema = "Airline_ID integer , Name String , Alias String , IATA string , ICAO string , Callsign String , Country string , Active string"

    Airline_df = spark.read.csv(path=r"C:\Users\admin\PycharmProjects\Airport_Project\Input\airline.csv" , schema=schema)
    Airline_df.printSchema()
    # Airline_df.show(10)


    schema2 = "Airport_ID integer , Name string , City string , Country string , IATA string , ICAO string , Latitude string ,Longitude string , Altitude integer,Timezone string ,DST string ,Tz string,Type string ,Source string"

    Airport_df = spark.read.csv(path=r"C:\Users\admin\PycharmProjects\Airport_Project\Input\airport.csv", schema=schema2)
    Airport_df.printSchema()
    # Airport_df.show(10)

    # schema3 = "Airline string,Airline_ID integer,Source_airport string,Source_airport_ID int , Destination_airport string , Destination_airport_ID integer , Codeshare string , Stops integer , Equipment string"

    Routes_df = spark.read.parquet(r"C:\Users\admin\PycharmProjects\Airport_Project\Input\routes.snappy.parquet")
    Routes_df.printSchema()
    # Routes_df.show()

    Planes_df = spark.read.csv(r"C:\Users\admin\PycharmProjects\Airport_Project\Input\plane.csv",inferSchema= True,header=True , sep='')
    Planes_df.printSchema()
    # Planes_df.show()


#Q1) 1. In any of your input file if you are getting \N or null values in your column and that column is of string type then put default value as "(unknown)" and if column is of type integer then put -1

    # ALdf2 = Airline_df.na.fill("Unknown").na.fill(value=-1).replace("\\N","Unknown")
    # ALdf2.show()


#Q2) 2. find the country name which is having both airlines and airport

    # Airport_df.join(Airline_df ,on=Airport_df.Country == Airline_df.Country , how='inner').select(Airport_df.Country).distinct().show()


#Q3) 3. get the airlines details like name, id,. which is has taken takeoff more than 3 times from same airport
    df5 = Routes_df.join(Airline_df, on=Airline_df.Airline_ID == Routes_df.airline_id).groupBy(Airline_df.Name,Airline_df.Airline_ID,Routes_df.src_airport).count()
    df6 = df5.select("*").filter(col("Count") > 3)
    # df6.show()

#Q4) 3. get airport details which has minimum number of takeoffs and landing.

    Take_off = Routes_df.join(Airport_df,on= Airport_df.Airport_ID==Routes_df.src_airport_id,how="inner").select(Routes_df.src_airport,Airport_df.Name).groupBy(Routes_df.src_airport,Airport_df.Name).count()

    wind = Window.partitionBy("count").orderBy(col("count").asc())

    take_offs = Take_off.withColumn("Rank",rank().over(wind)).distinct()
    Min_Take_Offs=take_offs.filter(col("Rank")==1)
    # Min_Take_Offs.show(15,truncate=False)


    Landing = Routes_df.join(Airport_df, on=Airport_df.Airport_ID==Routes_df.dest_airport_id).select(Routes_df.dest_airport,Airport_df.Name).groupBy(Routes_df.dest_airport,Airport_df.Name).count()

    wind1 = Window.partitionBy("count").orderBy(col("count").asc())

    landing = Landing.withColumn("Rank",rank().over(wind1)).distinct()
    Min_Landing = landing.filter(col("Rank")==1)
    # Min_Landing.show(10,truncate=False)

    joi2 = Min_Landing.join(Min_Take_Offs,on=Min_Landing.Rank==Min_Take_Offs.Rank,how="inner")
    # joi2.show(truncate=False)


#Q5) 4. get airport details which is having maximum number of takeoff and landing.

    Take_off1 = Routes_df.join(Airport_df, on=Airport_df.Airport_ID==Routes_df.src_airport_id).select(Routes_df.src_airport,Airport_df.Name).groupBy(Routes_df.src_airport,Airport_df.Name).count()

    wind3 = Window.orderBy(col("count").desc())
    take_offs=Take_off1.withColumn("Rank",rank().over(wind3)).distinct()
    Max_take_off = take_offs.filter(col("Rank")==1).distinct()
    # Max_take_off.show(10,truncate=False)

    Landing1 = Routes_df.join(Airport_df,on=Airport_df.Airport_ID==Routes_df.dest_airport_id).select(Routes_df.dest_airport,Airport_df.Name).groupBy(Routes_df.dest_airport,Airport_df.Name).count()

    Wind4= Window.orderBy(col("count").desc())

    landing1 = Landing1.withColumn("Rank",rank().over(Wind4)).distinct()

    Max_landing = landing1.filter(col("Rank")==1)
    # Max_landing.show(10,truncate=False)


    joi = Max_landing.join(Max_take_off,on= Max_landing.Rank == Max_take_off.Rank,how="inner")
    # joi.show(truncate=False)



#Q6) 5. Get the airline details, which is having direct flights. details like airline id, name, source airport name, and destination airport name

    Air_Route = Airline_df.join(Routes_df,on=Routes_df.airline_id==Airline_df.Airline_ID,how='leftouter').select(Airline_df.Airline_ID,Airline_df.Name,Routes_df.src_airport,Routes_df.src_airport_id,Routes_df.dest_airport_id,Routes_df.dest_airport,Routes_df.stops).filter(col("stops")==0).distinct()
    # Air_Route.show()

    pros = Air_Route.join(Airport_df,on=Air_Route.src_airport_id==Airport_df.Airport_ID,how='inner').select(Air_Route.Airline_ID,Air_Route.Name,Routes_df.src_airport_id,Routes_df.dest_airport_id,Routes_df.dest_airport,Routes_df.src_airport,Routes_df.stops).distinct()
    # pros.show()

    dest = pros.join(Airport_df,on=pros.dest_airport_id==Airport_df.Airport_ID,how="inner").select(pros.Airline_ID,pros.Name,pros.src_airport,pros.dest_airport,pros.stops).distinct()

    # dest.show()

