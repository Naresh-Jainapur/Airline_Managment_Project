from pyspark.sql import SparkSession
from common.readutil import ReadDataUtil

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("Sample").getOrCreate()

    rdu = ReadDataUtil()
    df = rdu.readCsv(spark=spark,path=r"C:\Users\admin\PycharmProjects\Learning_pyspark\Pyspark_Lec\Input\CSV\CSV_With_Header.csv")
    df.printSchema()
    df.show()