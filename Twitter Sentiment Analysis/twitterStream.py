from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    #print(pwords)
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    #print(counts)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    pos=[]
    neg=[]
    #time=[]
    
    for word in counts:
        for i in word:
            if(i[0]=='positive'):
                pos.append(i[1])
            else:
                neg.append(i[1])
    
    '''
    for t in range(len(counts)):
        time.append(t)
    '''   
    fig=plt.figure()   
    plt.plot(pos, 'bo-')
    plt.plot(neg, 'go-')
    
    plt.xlabel('Time')
    plt.ylabel('Count')
    plt.axis([0, 12, 0, max(max(pos),max(neg))*1.2])
    plt.legend(['P','N'], loc='upper left')
    #plt.show()
    fig.savefig('twitter1.png')


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    
    file=open(filename)
    words=file.read()
    wordlist=words.split('\n')
    #file.close()
    return wordlist

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    def updateFunction(newval, runcount):
        if runcount is None:
            runcount=0
        return sum(newval, runcount)

    tweets = kstream.map(lambda x: x[1])

    word1=tweets.flatMap(lambda x: x.split(" "))
    #word1=word1.filter(lambda word: (word in pwords) or (word in nwords))
    pp=word1.map(lambda word: ('positive', 1) if (word in pwords) else ('positive', 0))
    np=word1.map(lambda word: ('negative', 1) if (word in nwords) else ('negative', 0))
    pairs=pp.union(np)
    wcount=pairs.reduceByKey(lambda x, y: x+y)
    total=pairs.updateStateByKey(updateFunction)
    total.pprint()
    
    counts = []
    wcount.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts



if __name__=="__main__":
    main()
