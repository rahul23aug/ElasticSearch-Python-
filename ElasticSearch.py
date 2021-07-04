import numpy as np, pandas as pd, json
import requests
from elasticsearch import Elasticsearch, helpers

# wrapper class 

class SimpleElastic():
  def __init__(self, host= 'localhost',port =9200):
    self.node_host = host + ':' + str(port)
    self.es = Elasticsearch([{'host': host, 'port': port}])
    self.toggle = 1
    if self.ping():
      print("[*] Connection to Node: {}:{} - Successful!".format(host,port))
    else:
      print("[*] Node unavailable!")    
  def ping(self):
    if self.es.ping():
      return True
    else:
      return False

  def df_to_json(self, x, bulk):
    if not bulk:
      '''one by one insertion : converts single dataset rows to json format returns the same'''
      d = x.to_json()
      e = json.loads(d)
      return e
    else:
      '''converts dataframe to json doc for bulk insert '''
      d = x.to_json(orient = 'records')
      e = json.loads(d)
      return e
      
  def async_generator(self,data,_index,mappings, id_=None):
    json_list = self.df_to_json(data,True)
    if id_ is None:
      i = 0
      for doc  in json_list:
        yield{
          "_index" : _index,
          "_id"   : i,
          "_source": doc,
          "_mapping": mappings
        }
        i+=1
    else:
      i = 0
      for doc  in json_list:
        yield{
          "_index" : _index,
          "_id"   : id_[i],
          "_source": doc,
          "_mapping": mappings
        }
        i+=1
 
  def get_indices(self):
    p=[]
    for index in self.es.indices.get("*"):
      p.append(index)
    return p

  def insert(self,data,_index,mappings,async_ = True, chunk_size = 15, read_timeout = 20):
    '''Takes a DataFrame object and inserts into index'''
    assert type(data) == type(pd.DataFrame([])), 'Data must be a pandas dataframe object'
    if async_:
      try:
        if _index not in self.get_indices():
          resp = self.es.indices.create(index = _index, body= mappings, ignore = 400)    
        response = helpers.bulk(self.es,self.async_generator(data, _index , mappings), chunk_size=chunk_size, request_timeout = read_timeout )
        print(response)
      except Exception as e:
        print(e)
    else:
      if self.ping():
        if _index not in self.get_indices():
          resp = self.es.indices.create(index = _index, body= mappings, ignore = 400)    
        for i in range(data.shape[0]):
          stat = self.es.index(index='test0', id=i+1, body=self.df_to_json(data.loc[i], False))
          print('[+] Indexing ',i,' as id: ',stat['_id'] ,' Status : ', stat['result'], ' Version : ', stat['_version'])

      else:
        print("[!]Error: No Connection to Node!")
        

  def delet_index(self, index):
    '''Deletes an index '''
    self.es.indices.delete(index = index, ignore =[400,404])

  def search_index(self, index, body):
    res = self.es.search(index=index, body=body)
    for i in res['hits']['hits']:
      print(i['_source'])
    return res

  def update_index_by_id(self,index_, body, id_ ):
    self.es.update(index=index_, body = body, id = id_ )

  def delete_by_id(self, index_,ids):
    body = {"query": {"terms": {"_id":ids}}}
    resp = self.es.delete_by_query(index = index_, body = body)
	
  def add_ingest_pipeline(self,pname, body): #Brokenn Implementation
    if (1):
      pass
    if self.toggle and pname not in requests.get(self.node_host + '/_ingest/pipeline/').content:
      response = requests.put('http://' + self.node_host + '/_ingest/pipeline/'+ pname, data = body)
      print(response)
      self.toggle = 0
      '''PUT /_ingest/pipeline/indexed_at
{
"description": "Creates a timestamp when a document is initially indexed",
"processors": [
{
"set": {
"field": "_source.Insert_date",
"value": "{{_ingest.timestamp}}"
}
}
]
}'''
  

if __name__ == "__main__":

# util functions
  def price_handler(x):
    y=x.split(" ")
    if 'Lacs' in y[2]:
      return float(y[1]) * 100000
    if 'Crores' in y[2]:
      return float(y[1]) * 10000000


  def bhk_preprocessor(x): 
    x = int(x.split("BHK")[0])
    return x

  dataset1 = pd.read_csv('.\\new_properties.csv', index_col= 'Unnamed: 0')
  dataset1.drop(['Link','Area', 'Pocession', 'Extra',    'Basic_amenities',    'Lifestyle_ammenities',    'More_deets'],axis = 1 , inplace= True)
  dataset1.drop(dataset1[dataset1['Price']=='Price On Request'].index,axis=0,inplace=True)
  dataset1.index = [i for i in range(dataset1.shape[0])]
  dataset = pd.read_csv('.\\resale_properties.csv', index_col= 'Unnamed: 0')
  dataset1['BHK'] = dataset1['BHK'].apply(lambda x : bhk_preprocessor(x))
  dataset1['BHK'] = dataset1['BHK'].astype('int')
  dataset1['Price'] = dataset1['Price'].apply(lambda x : price_handler(x))
  df = pd.read_csv('.\\Walmart_Store_sales.csv')
  df['Date'] = pd.to_datetime(df['Date'], yearfirst = True)
  
  mappings = {
            "settings" : {
            "number_of_shards" : 1,
            "number_of_replicas":1,
            "index.default_pipeline" : "indexed_at"
                          },
            "mappings" : {"properties": 
            {
            "Proj_name": {"type":"text" },
            "Adderess" :{"type": "text"},
            "BHK" : {"type": "integer"},
            "Property_type" : {"type": "text"},
            "Area_type" : {"type":"text"},
            "Construction_status" : {"type":"text"},
            "RERA_status" : {"type":"text"},
            "Price" : {"type":"float"},
            }
                         }
            }
  pipeline = {
  "description": "Adds indexed_at timestamp to documents",
  "processors": [
    {
      "set": {
        "field": "_source.Insert_date",
        "value": "{{_ingest.timestamp}}"
             }
    }
                ]
             }
  search_body = {"query": {"match": {'Proj_name': "Ambrosia"}}}

  mappings1 = {
            "settings" : {
            "number_of_shards" : 1,
            "number_of_replicas":1
                          },
            "mappings" : {"properties": 
            {
            "Store": {"type":"integer" },
            "Date" :{"type": "date", 
			'format' :'yyyy-MM-dd', 
			'ignore_malformed': 'true'},
            "Weekly_Sales" : {"type": "float"},
            "Holiday_Flag" : {"type": "integer"},
            "Temperature" : {"type":"text"},
            "Fuel_Price" : {"type":"float"},
            "CPI" : {"type":"float"},
            "Unemployment" : {"type":"float"},
            }
                         }
            }

  date_range_query =  {
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "Date": {
                            "gte": "2010-02-05",
                            "lte": "2010-02-12"
                        }
                    }
                }
            ]
        }
    }
}
#another way for simpler range queries
  date_range_query2 = {
    "query": {
                    "range": {
                        "Date": {
                            "gte": "2010-02-05",
                            "lte": "2010-02-12"
                        }
                    }
                }    
}

#aggregation on weekly sales and cpi returns avg of weekly Sales and CPI using
#multi aggregation 
  agg = {
  "aggs": {
    "avg-of-sales": {
      "avg": {
        "field": "Weekly_Sales"
      }
    },
    "avg-of-cpi": {
      "avg": {
        "field": "CPI"
      }
    }
  }
  }

  update_id = {'doc': {'BHK':4}}



  SE = SimpleElastic()
  #SE.add_ingest_pipeline('indexed_at', pipeline) #broken implementation
  #SE.insert(dataset1,'test0',mappings,async_ = False, chunk_size = 15, read_timeout = 20) # async_ and following arguments are used only when async == True
  #SE.insert(dataset1,'test1',mappings,async_ = True, chunk_size = 15, read_timeout = 20)
  #SE.search_index('test1', search_body)
  #SE.delete_by_id('test0', [0,1])
  #SE.insert(df,'walmart_sales',mappings1,async_ = False, chunk_size = 200, read_timeout = 20) # async_ and following arguments are used only when async == True
  #SE.insert(df,'walmart_sales_bulk',mappings1,async_ = True, chunk_size = 200, read_timeout = 20) # async_ and following arguments are used only when async == True
  #Date range Query
  SE.search_index('walmart_sales_bulk', date_range_query)
  SE.search_index('walmart_sales_bulk', date_range_query2)
  #Multiple Aggregation on fields
  print(SE.search_index('walmart_sales_bulk', agg)['aggregations'])
  #Update documents
  print("Before Updating")
  SE.search_index('test1', search_body)
  SE.update_index_by_id('test1', update_id, 0 )
  print("After Updating")
  SE.search_index('test1', search_body)
  