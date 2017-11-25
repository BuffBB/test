# -*- coding: utf-8 -*-
import json
import sys 
from elasticsearch import Elasticsearch
sys.path.append("folder_containing_my_python_util_script")
import utils


class EsMananger:
    es = Elasticsearch(['99.12.226.127'], http_auth=('admin', 'MuyAkyPVKEUyxU3Mc9W_JDpPxCoaRDDw'), port='9280')
    def createIndex(self, indexName):
        self.es.indices.create(index=indexName, ignore=400)
    def put(self, indexName, datatype, data):
        self.es.index(index=indexName, doc_type=datatype, body=data)
    def put1(self, indexName, datatype,data,dataid):
        self.es.index(index=indexName,doc_type=datatype,body=data,id=dataid)
    def health(self):
        return self.es.cluster.health()
    def getdata(self,indexName, datatype):
        return self.es.get(index=indexName, doc_type=datatype)
    def getdataID(self,indexName, datatype, dataid):
        return self.es.get(index=indexName, id=dataid, doc_type=datatype)
#   def get(self, index, id, doc_type='_all', params=None):
#res = es.get(index="my-index", doc_type="test-type", id=01)
#es.index(index="my-index",doc_type="test-type",id=01,body={"any":"data01","timestamp":datetime.now()})
def createLoginLogs():
    log = {}
    reducedAppID = utils.getRandomStr()
    loginTime = utils.getRandomTime()

    log['ReducedAppID'] = reducedAppID
    log['LoginTime'] = loginTime

    return log


def save2ES(num):
    es = EsMananger()
    for eachNum in range(num):
        log = createLoginLogs()
        es.put("cdl_data2", "aaa", log)

def save2ES1(num):
    es = EsMananger()
    for eachNum in range(num):
        log = createLoginLogs()
        es.put1("cdl_data1", "aaa", log,eachNum)
        
def save2ESAndFile(num):
    es = EsMananger()
    f =  open('C:/Users/80234775/Desktop/test.txt', 'w')

    for eachNum in range(num):
        log = createLoginLogs()
        es.put("cdl_data2", "aaa", log)
        f.write(json.dumps(log)+"\n")

    f.close()

def save2File(num):
    f = open('C:/Users/80234775/Desktop/test.txt', 'w')
    for eachNum in range(num):
        log = createLoginLogs()
        f.write(json.dumps(log)+"\n")
        
    f.close()

def getfromES(num):
    es = EsMananger()
    #for eachNum in range(num):
        #res = es.getdataID("cdl_data1","aaa",eachNum)
    res = es.getdataID("cdl_data","68","015100122655")
    print(res)

if __name__ == '__main__':
    print("hello world")
    es = EsMananger()

    save2File(50000)
    save2ES1(50)
   # getfromES(1)
    print('Search all...',  flush=True)
    _query_all = {
      'query': {
        'match_all': {}
                }
    }
    _searched = es.es.search(index='cdl_data1',doc_type='rating', body=_query_all)
    rating_dict = { 'start': 0 };

   # res = es.es.search(index="cdl_data1") 
    #print(_searched,flush=True)
#    for hit in _searched['hits']['hits']:
#        print(hit['_source'], flush=True)
    


    #data=es.es.get(index='cdl_data1', id='015100111474', doc_type='rating')
    #print(data)
    
    
    for i in range(1000):
        data=es.es.get(index='cdl_data1', id=i, doc_type='rating')
        rating_dict[str(i)] = data['_source'];
        #print(data['_source'])
    for i in range(1000):
        try:
            index=rating_dict.get(str(i))['loginID']        
            personas=es.es.get(index='cdl_data2',id=index,doc_type='rating')
            print(personas)
        except:  
            print("无此数据")

       
        
        
        #self, index, id, doc_type='_all', params=None
    # save2ES(2000)
    # save2ESAndFile(5000)

    # createLoginLogs()
    # es.put("login-screen", "aaaa", {"aa":"bb"})
