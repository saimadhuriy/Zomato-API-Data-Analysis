from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("myApp").config("spark.mongodb.input.uri","mongodb://127.0.0.1/rest_db.rest_collection").getOrCreate()
    rest_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

    # Stop the session
    spark.stop()

    5e842254c5b65dd0c5d65ed5
    idUSKBN21F0G9
     idINKBN21J4AT
     idINKBN21J4AT
     idINKBN21J4EK
     20200401 p54g3b.html
     20200401 p54g3b.html
     idUSKBN21J4N6
     fydxrvizzvg6pda4tgphjvq5wa
     4v7rz2qenrgnvg4u5yocqm2u3i
     raenkr4geretro4yv27lypcjii story
     aotj35bkdjg2daohplci6zjspy
     2z2mfv4kwjdgxcfw2bu7ml437y -- 25
     
     120040100936_1
     120040100936_1
     120040101005_1
     a4403826
     a9440126
     
     #([0-9a-z]){25,} | (id[A-Z0-9]{11}) | (2020[0-9]{4}) | (\d{12})_(\d{1}) | (p\d{2}g\d{1}b) | (a\d{7})
     L.A. los angeles
     UK united kingdom
     UN united nations
     U.N. united nations
     US united states
     NE nebraska
     MN minnesota
     d.c. district of columbia
     u.s. united states
     U.S. united states
     PPE personal protective equipment
     NJ new jersey
     EU europian union
     ICU intensive care unit
     LGBT minority
     UAE united arab emirate
     PTSD post traumatic stress disorder
     WA washington
     GA georgia
     USA united states
     UNICEF united nations children's fund
     CA Canada
     N.D. no date
     S.C. south carolina
     UTI urinary track infection
     UIC illinois university
     USDA unites states department of agriculture
     NSA national security agency
     IMF international monetary fund
     SA south africa
     PRC china
     
     
     
     

     