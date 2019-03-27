import sys
import math
import random
import pandas as pd
import numpy as np

choice = sys.argv[1]

def load(data):
    
    global abdict, edgelist, bud
    bud = data.dropna()    
    
    buddict = dict(zip(bud.Advertiser, bud.Budget))
    abdict = dict(zip(bud.Advertiser, bud.Budget))

    data.columns = ['Advertiser', 'Keyword', 'Bid_Value', 'Budget']
    data['Advertiser_Bids'] = list(zip(data.Advertiser, data.Bid_Value))

    bids = data.groupby("Keyword")["Advertiser_Bids"]
    edgelist = {key:list(value) for key, value in bids}

def greedy(queries):
    
    buddict = dict(zip(bud.Advertiser, bud.Budget))
    
    for key, value in edgelist.items():
        edgelist[key] = sorted(value, key = lambda l: l[1], reverse=True)
        
    for q in queries:
        
        for bidder in edgelist[q]:
            
            adv = bidder[0]
            bid = bidder[1]
            budget = buddict[adv]
            
            if budget >= bid:
                buddict[adv] -= bid
                break
    
    rev = 0.0
    for adv in buddict:
        rev += abdict[adv] - buddict[adv]
        
    return round(rev, 2)

def msvv(queries):
    
    buddict = dict(zip(bud.Advertiser, bud.Budget))
    
    for q in queries:
        
        msbid = 0.0
        finbid = 0.0
        finadv = "_"
        
        for bidder in edgelist[q]:
            
            adv = bidder[0]
            bid = bidder[1]
            
            budtot = abdict[adv]
            rem = buddict[adv]
            
            if rem < bid:
                continue
                
            y = (budtot - rem) / budtot
            scale = (1 - math.exp(y - 1))
            sbid = bid * scale
            
            if sbid > msbid:
                msbid = sbid
                finbid = bid
                finadv = adv
                
        
        if finadv != "_":
            buddict[finadv] -= finbid
            
    rev = 0.0
    for adv in buddict:
        rev += abdict[adv] - buddict[adv]
        
    return round(rev, 2)

def balance(queries):
    
    buddict = dict(zip(bud.Advertiser, bud.Budget))
    
    for q in queries:
        
        unspent = 0.0
        finbid = 0.0
        finadv = "_"
        
        for bidder in edgelist[q]:
            
            adv = bidder[0]
            bid = bidder[1]
            
            budtot = abdict[adv]
            rem = buddict[adv]
            
            if rem < bid:
                continue
            
            if rem > unspent:
                unspent = rem
                finbid = bid
                finadv = adv        
        
        if finadv != "_":
            buddict[finadv] -= finbid   
    
    rev = 0.0
    for adv in buddict:
        rev += abdict[adv] - buddict[adv]
        
    return round(rev, 2)

def main():
    data = pd.read_csv('./bidder_dataset.csv')
    load(data)

    query = [line.rstrip('\n') for line in open('./queries.txt')]
    alg = 0.0
    opt = 0.0
    
    for adv in abdict:
        opt += abdict[adv]
        
    random.seed(0)
        
    if choice == 'greedy':
        gr = greedy(query)
        print("Revenue: ", format(gr, '.2f'))
        for i in range(100):
            random.shuffle(query)
            alg += greedy(query)
            
    elif choice == 'msvv':
        mr =  msvv(query)
        print("Revenue: ", format(mr, '.2f'))
        for i in range(100):
            random.shuffle(query)
            alg += msvv(query)
            
    elif choice == 'balance':
        br = balance(query)
        print("Revenue: ", format(br, '.2f'))
        for i in range(100):
            random.shuffle(query)
            alg += balance(query)  
    
    alg /= 100
    cr = alg / opt
    print("Competitive Ratio: ", format(cr, '.2f'))
    
    
if __name__ == "__main__":
    main()

