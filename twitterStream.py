from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt
import string


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    #print nwords
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)
    


def make_plot(counts):
    pos_count = []
    neg_count = []
    for temp in counts:
	#print temp
	for t in temp:
	    if t[0] == "positive":
		pos_count.append(t[1])
	    else:
		neg_count.append(t[1])
    #print positive
    #print negative
    pos, = plt.plot(range(len(pos_count)),pos_count,"bo-", label="positive")
    neg, = plt.plot(range(len(neg_count)),neg_count,"go-", label="negative")
    plt.xticks(range(len(counts)))
    plt.legend(handles=[pos,neg],loc=2)
    plt.xlabel("Time step")
    plt.ylabel("Word count")
    #print("Before printing plots!")
    plt.show()


def load_wordlist(filename):
    req_text = open(filename,"rU")
    #req_list = req_text.flatMap(lambda x : x.split("\n"))
    #pprint(req_list)
    return req_text.read().split("\n")


def pos_or_neg(word, pwords):
    if word in pwords:
	return("positive",1)
    else:
	return("negative",1)

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount) 

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    
    

    pos_neg = tweets.flatMap(lambda word:word.split(" ")).filter(lambda word:True if word in pwords or word in nwords else None).map(lambda word:pos_or_neg(word,pwords)).reduceByKey(lambda x,y : int(x)+int(y))
    
    counts = []
    pos_neg.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    pos_neg.updateStateByKey(updateFunction)
    pos_neg.pprint()
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
