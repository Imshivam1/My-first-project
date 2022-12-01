##sample for storing data in databricks and calling restapi

from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import http
import json

#spark = SparkSession \
#.builder \
#.appName("DataCleansing") \
#
#.getOrCreate()


conn = http.client.HTTPSConnection("api.postcodes.io")
payload = ''
headers = {
          'Cookie': '__cfduid=d2e270bea97599e2fbde210bf483fcd491615195032'
          }
conn.request("GET", "/random/postcodes", payload, headers)

res = conn.getresponse()
data = res.read().decode("utf-8")
jsondata = json.loads(json.dumps(data))
df = spark.read.json(sc.parallelize([jsondata]))
df_temp = df.selectExpr("string(status) as status","result['country'] as country", "result['european_electoral_region'] as european_electoral_region", 
"string(result['latitude']) as latitude", "string(result['longitude']) as longitude", "result['parliamentary_constituency'] as parliamentary_constituency", 
"result['region'] as region","'' as vld_status","'' as vld_status_reason")
df_temp.write.format("delta").mode("append").saveAsTable(f"{table_name}")


'''def convert_single_object_per_line(json_list):
json_string = ""
for line in json_list:
json_string += json.dumps(line) + "\n"
return json_string

def parse_dataframe(json_data):
r = convert_single_object_per_line(json_data)
mylist = []
for line in r.splitlines():
mylist.append(line)
rdd = spark.sparkContext.parallelize(mylist)
df = SQLContext.jsonRDD(rdd)
return df

url = "https://mylink"
response = urlopen(url)
data = str(response.read())
json_data = json.loads(data)
df = parse_dataframe(json_data)'''
