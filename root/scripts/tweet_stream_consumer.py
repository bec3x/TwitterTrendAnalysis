from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
from os import environ
from configparser import ConfigParser

config = ConfigParser()
config.read('../conf/exit.conf')

def sum_all_tags(new_values, last_sum):
    if last_sum is None:
        return sum(new_values)
    return sum(new_values) + last_sum

def getSparkSessionInstance(spark_context):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SQLContext(spark_context)
    return globals()['sparkSessionSingletonInstance']

def process_hashtags(time, rdd):
    try:
        spark_sql = getSparkSessionInstance(rdd.context)
        rowRdd = rdd.map(lambda tag: Row(hashtag=tag[0], frequency=tag[1]))
        hashtagsDataFrame = spark_sql.createDataFrame(rowRdd)
        hashtagsDataFrame.createOrReplaceTempView("hashtags")
        hashtagCountsDataFrame = spark_sql.sql("select hashtag, frequency from hashtags order by frequency desc limit 10")
        update_txt(hashtagCountsDataFrame)
    except:
        e, p, t = sys.exc_info()
        print(p)

def update_txt(hastagCountsDataFrame):
    top_hashtags = {}

    for hashtag, frequency in hastagCountsDataFrame.collect():
        top_hashtags[hashtag] = frequency

    output_file = '..\\output\\output.txt'
    with open(output_file, 'w+') as f:
        for key, value in top_hashtags.items():
            try:
                f.write(f'{key}: {value}\n')
            except:
                continue

if __name__ == '__main__':
    environ['PYSPARK_PYTHON'] = sys.executable
    environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    pyspark_environ = config['Resources']['pyspark_environ']
    environ['PYSPARK_SUBMIT_ARGS'] = pyspark_environ

    sparkConf = SparkConf("TweetAnalysis")
    sparkConf.setMaster("local[2]")
    sparkConf.setAppName("Twitter Analysis")

    sc = SparkContext(conf=sparkConf)
    sc.setLogLevel("ERROR")

    input_dir = config["Resources"]["input_dir"]
    ssc = StreamingContext(sc, 60)
    ssc.checkpoint("checkpointTwitter")

    tweets = ssc.textFileStream(input_dir)

    words = tweets.flatMap(lambda y: y.replace(',', '').replace('.', '').split(" "))
    hashtags = words.filter(lambda tag: len(tag) > 2 and '#' == tag[0]).countByValue().updateStateByKey(sum_all_tags)

    hashtags.foreachRDD(process_hashtags)
    ssc.start()
    ssc.awaitTermination(10000)
