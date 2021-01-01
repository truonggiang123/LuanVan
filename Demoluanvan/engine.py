from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import explode, col
from pyspark.sql.types import DoubleType
import pandas as pd
from pyspark.sql.functions import explode, split, col, trim, translate,length, regexp_replace
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import IndexToString

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationEngine:

    def __trainmodelgoiybenh(self):
        als = ALS(maxIter=10, 
        regParam = 0.1,
        rank = 16,
        alpha = 10.0,
        seed = 8427,
        implicitPrefs = True,
        userCol="trieuchungIndex", 
        itemCol="chandoanIndex", 
        ratingCol="rating", 
        coldStartStrategy="drop")
        
        self.model= als.fit(self.train)
        
    def __trainmodelgoiythuoc(self):
        als = ALS(maxIter=10, 
        regParam = 0.1,
        rank = 16,
        alpha = 10.0,
        seed = 8427,
        implicitPrefs = True,
        userCol="chandoanIndex", 
        itemCol="tenhhIndex", 
        ratingCol="rating", 
        coldStartStrategy="drop")
        
        self.modelgoiythuoc = als.fit(self.traingoiythuoc)
         
    def __init__(self, spark):
        self.spark = spark
        df_raw = spark.read.format("csv").option("delimiter", ",") \
        .option("quote", "\"").option("escape", "\"") \
        .option("header", "true").option("inferSchema", "true") \
        .load("datasetfinaltotal.csv")
        df_raw1 = df_raw.dropna(how='any')
        df_raw1.show()
        #model goi y benh
        df_raw2 = df_raw1.select('lydo','chandoan', translate(col('lydo'),".;",",,").alias('trieuchung'))
        df_raw2 = df_raw2.select("trieuchung","chandoan").distinct()
        df_raw2 = df_raw2.withColumn('trieuchung', explode(split('trieuchung',',')))
        df_raw3 = df_raw2.select('trieuchung','chandoan').distinct()
        df_raw3 = df_raw3.withColumn('trieuchung', trim(col('trieuchung')))
        df_raw3 = df_raw3.select("trieuchung","chandoan").distinct()
        df_raw3 = df_raw3.filter(col('trieuchung')!="")
        df_raw3 = df_raw3.filter(length(regexp_replace("trieuchung", " ", " "))>2)
        df_raw3.show()
        
        pddataframe = df_raw3.toPandas()
        dfpd = pd.crosstab(pddataframe['trieuchung'], pddataframe['chandoan'])
        flattened = pd.DataFrame(dfpd.to_records())
        
        
        flattend1 = flattened.melt(id_vars=["trieuchung"], var_name="chandoan", value_name="rating")
        df_final = spark.createDataFrame(flattend1)
        self.df_final = df_final
        userIndexer = StringIndexer(inputCol='trieuchung',outputCol='trieuchungIndex').fit(df_final)
        itemIndexer = StringIndexer(inputCol='chandoan',outputCol='chandoanIndex').fit(df_final)
        pipeline = Pipeline(stages=[userIndexer, itemIndexer])
        df_testfinal = pipeline.fit(df_final).transform(df_final)
        df_testfinal.show()
        self.df_testfinal = df_testfinal
        train, test = df_testfinal.randomSplit([0.8,0.2])
        self.train = train
        self.test = test
        self.__trainmodelgoiybenh()
        userRecs = self.model.recommendForAllUsers(10)
        flatUserRecs = userRecs.withColumn("trieuchungandrating",explode(userRecs.recommendations)).select('trieuchungIndex','trieuchungandrating.*')
        userIndexer = StringIndexer(inputCol='trieuchung',outputCol='trieuchungIndex').fit(self.df_final)
        itemIndexer = StringIndexer(inputCol='chandoan',outputCol='chandoanIndex').fit(self.df_final)
        itemConverter = IndexToString(inputCol='chandoanIndex', outputCol='chandoan',labels=itemIndexer.labels)
        userConverter = IndexToString(inputCol='trieuchungIndex', outputCol='trieuchung', labels=userIndexer.labels)
        convertedUserRec = Pipeline(stages=[userConverter,itemConverter]).fit(self.df_testfinal).transform(flatUserRecs)
        self.convertedUserRec = convertedUserRec
        #mo hinh goi y thuoc
        df_goiythuoc = df_raw1.select('chandoan','tenhh').distinct()
        df_goiythuoc.show()
        
        pddataframegoiythuoc = df_goiythuoc.toPandas()
        dfpdgoiythuoc = pd.crosstab(pddataframegoiythuoc['chandoan'], pddataframegoiythuoc['tenhh'])
        flattenedgoiythuoc = pd.DataFrame(dfpdgoiythuoc.to_records())
        
        
        flattendgoiythuoc1 = flattenedgoiythuoc.melt(id_vars=["chandoan"], var_name="tenhh", value_name="rating")
        df_finalgoiythuoc = spark.createDataFrame(flattendgoiythuoc1)
        userIndexergoiythuoc = StringIndexer(inputCol='chandoan',outputCol='chandoanIndex').fit(df_finalgoiythuoc)
        itemIndexergoiythuoc = StringIndexer(inputCol='tenhh',outputCol='tenhhIndex').fit(df_finalgoiythuoc)

        pipeline = Pipeline(stages=[userIndexergoiythuoc, itemIndexergoiythuoc])
        df_testfinalgoiythuoc=pipeline.fit(df_finalgoiythuoc).transform(df_finalgoiythuoc)
        traingoiythuoc, testgoiythuoc = df_testfinalgoiythuoc.randomSplit([0.8,0.2])
        self.traingoiythuoc=traingoiythuoc
        self.testgoiythuoc=testgoiythuoc
        self.__trainmodelgoiythuoc()
        userRecsgoiythuoc = self.modelgoiythuoc.recommendForAllUsers(20)
        flatUserRecsgoiythuoc = userRecsgoiythuoc.withColumn("chuandoanandrating",explode(userRecsgoiythuoc.recommendations)).select('chandoanIndex','chuandoanandrating.*')
        userConvertergoiythuoc = IndexToString(inputCol='chandoanIndex', outputCol='chandoan',labels=userIndexergoiythuoc.labels)
        itemConvertergoiythuoc = IndexToString(inputCol='tenhhIndex', outputCol='tenhh',labels=itemIndexergoiythuoc.labels)
        convertedUserRecgoiythuoc = Pipeline(stages=[userConvertergoiythuoc,itemConvertergoiythuoc]).fit(df_testfinalgoiythuoc).transform(flatUserRecsgoiythuoc)
        self.convertedUserRecgoiythuoc=convertedUserRecgoiythuoc
        
        
        

        
    def get_movie_recomment_from_user(self, trieuchung):
        result = self.convertedUserRec.filter(self.convertedUserRec['trieuchung'] == trieuchung).select('trieuchung','chandoan','rating')
        result.show()
        return result
        
    def get_thuoc_recomment_from_loaibenh(self, loaibenh):
        result = self.convertedUserRecgoiythuoc.filter(self.convertedUserRecgoiythuoc['chandoan'].contains(loaibenh)).select('chandoan','tenhh','rating')
        result.show()
        return result
    