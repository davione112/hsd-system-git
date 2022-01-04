print('starting spark!')
import os
import time

SCALA_VERSION = '2.12'
SPARK_VERSION = '3.1.2'

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION} pyspark-shell'

import findspark
import pyspark
findspark.init()

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from IPython.display import display, clear_output
from pyspark.sql.streaming import DataStreamReader

spark = (SparkSession
         .builder
         .appName('hsd-spark-kafka')
         .master('local[*]')
         .getOrCreate())

timestampformat = "yyyy-MM-dd HH:mm:ss"
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

df = (spark.readStream.format('kafka')
      .option("kafka.bootstrap.servers", "localhost:9092") 
      .option("subscribe", "detected") 
      .option("startingOffsets", "latest")
      .load())

from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType

schema_value = StructType(
    [StructField("author",StringType(),True),
    StructField("datetime",StringType(),True),
    StructField("raw_comment",StringType(),True),
    StructField("clean_comment",StringType(),True),
    StructField("label",IntegerType(),True)])

df_json = (df
           .selectExpr("CAST(value AS STRING)")
           .withColumn("value",f.from_json("value",schema_value)))
df_column = (df_json.select(f.col("value.author").alias("user"),
#                             f.col("value.date").alias("timestamp"),
                           f.to_timestamp(f.regexp_replace('value.datetime','[TZ]',' '),timestampformat).alias("timestamp"),
                           f.col("value.raw_comment").alias("raw_comment"),
                           f.col("value.clean_comment").alias("clean_comment"),
                           f.col("value.label").alias("label")
                           ))

# df_count = (df_column.groupBy('label').agg(f.count('label').alias('count'))
#             .withColumn('sentiment',f.when(df_column.label==1,'OFFENSIVE')
#                         .when(df_column.label==0,'CLEAN')
#                         .otherwise('HATE'))
# #             .withColumn('percentage', f.col('count') / f.length('count'))
# #             .withColumn('percentage', f.round('percentage',4))
#            .select(f.col('sentiment'),f.col('count')))

# # result.groupBy('label').agg(f.count('label').alias('count'))\
# #             .withColumn('sentiment',f.when(result.label==1,'OFFENSIVE')
# #                         .when(result.label==0,'CLEAN')
# #                         .otherwise('HATE'))\
# #             .withColumn('percentage', f.col('count') / result.count())\
# #             .withColumn('percentage', f.round('percentage',4)).show()

# df_hatespeech = (df_column.where(df_column.label != 0)
#             .withColumn('sentiment',f.when(df_column.label==1,'OFFENSIVE')
#                         .when(df_column.label==0,'CLEAN')
#                         .otherwise('HATE'))
#             .select('user','timestamp','raw_comment','sentiment'))

# df_haters = (df_hatespeech.groupBy('user')
#              .agg(f.count('sentiment').alias('most_hate_speech')).orderBy('most_hate_speech',ascending=False))
print('spark started!')

#=============================== Flask ==================================================
from flask import Flask, render_template, request, redirect, url_for
import time
import pandas as pd

app = Flask(__name__)
@app.route('/')
def home():
    return render_template('firstPage.html')

@app.route('/',methods= ['POST','GET'])
def start_str():
    if request.method == 'POST':
        if request.form['button1'] == 'Start Streaming':

#             ds_count = (df_count
#                         .writeStream
#                         .format("memory")
#                         .queryName("hsd_count")
#                         .outputMode("complete")
#             #   .option("kafka.bootstrap.servers", "localhost:9092") 
#             #   .option("topic", "detected") 
#             #   .option("checkpointLocation", "E:\download")\
#                         .start())
            return redirect(url_for('show_plot'))
    if request.method == 'GET':
        return render_template('firstPage.html')
@app.route('/startstreaming',methods= ['POST','GET'])
def show_plot():
#     sentiment = ['HATE','OFFENSIVE','CLEAN']
#     count = [3,3,94]

#     user = ['Tiềm Doan','Nguyễn Nhựt Minh','Đức hải','cồ cô ma']
#     most_hate_speech = [3,1,1,1]
#     df = pd.DataFrame({'user':user,'most_hate_speech':most_hate_speech})
    ds = (df_column 
            #       .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
                  .writeStream 
                  .format("memory") 
                  .queryName("hsd_query")
                  .outputMode("append")
            #   .option("kafka.bootstrap.servers", "localhost:9092") 
            #   .option("topic", "detected") 
            #   .option("checkpointLocation", "E:\download")\
                  .start())

    df = spark.sql(f"select * from {ds.name}").toPandas()
    return render_template('showPlotPage.html',
#                            values = sentiment, labels = count,
                           tables = [df.to_html(classes='data', header=True)])
        
if __name__ == "__main__":
    app.run(debug=True)