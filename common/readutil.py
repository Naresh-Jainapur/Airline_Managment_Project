

class ReadDataUtil:
    #def __init__(self):

    def readCsv(self,spark,path,schema=None,inferschema= True,header=True ,sep=","):
        """
        Returns new dataframe by reading provided csv file
        :param spark: spark session
        :param path: csv or directory path
        :param schema: provide schema, required when inferschema is false
        :param inferschema: detect if true: detect file schema else false: ignore auto detect schema
        :param header: if true: input csv file has header
        :param sep: default: "," specify separator if present in file
        :return:
        """
        if (inferschema is False) and (schema== None):
            raise Exception("Please provide inferschema as True else provide schema for given input file")
        readdf = spark.read.csv(path=path,inferSchema=inferschema,header=header,sep=sep)
        return readdf