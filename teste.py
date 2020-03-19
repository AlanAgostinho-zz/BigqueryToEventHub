
from google.cloud import storage
from datetime import datetime
import pytz



# parametros
bql = "select * from `b2w-bee-analytics.clean_order.deliveryline` where format_timestamp('%Y-%m-%d %H:%M:%S', (event_time)) >= replace_date_inicial and format_timestamp('%Y-%m-%d %H:%M:%S', (event_time)) < replace_date_final"
bq_table_source = bql.split('`')[1].split('.')
data_txt = 'clean_order_deliveryline/templates/deliveryline/data.txt'
sql_txt = 'clean_order_deliveryline/templates/deliveryline/sql.txt'
template = "clean_order_deliveryline/templates/deliveryline"
bucket_name = "ame-bucket"
jobname = "clean_deliveryline"
gcsPath = "gs://ame-bucket/clean_order_deliveryline/templates/deliveryline"

# parametros para validações não mexer
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
stats = storage.Blob(bucket=bucket, name=data_txt).exists(storage_client)
stats_sql = storage.Blob(bucket=bucket, name=sql_txt).exists(storage_client)
bucket = storage_client.get_bucket(bucket_name)

# olha a hora atual para salvar  e converte pra string
now = datetime.now(pytz.timezone("Brazil/East"))
timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

# read no template storage
blob_template = bucket.get_blob(template)
downloaded_template = blob_template.download_as_string()
downloaded_template = downloaded_template.decode('utf-8')

# validação se existir o arquivo data ele faz read caso false ele write arquivo com data 1900-01-01 default
if (stats == True):
    # read data from arquivo
    bucket = storage_client.get_bucket(bucket_name)
    blob_data = bucket.get_blob(data_txt)
    downloaded_data = blob_data.download_as_string()
    downloaded_data = downloaded_data.decode('utf-8')
else:
    bucket = storage_client.get_bucket(bucket_name)
    blob_data = bucket.blob(data_txt)
    blob_data.upload_from_string('2020-03-12 00:00:00')
    downloaded_data = "2020-03-12 00:00:00"

if (stats_sql == True):
    # read data from arquivo
    bucket = storage_client.get_bucket(bucket_name)
    blob_sql = bucket.get_blob(sql_txt)
    downloaded_sql = blob_sql.download_as_string()
    downloaded_sql = downloaded_sql.decode('utf-8')
else:
    bucket = storage_client.get_bucket(bucket_name)
    blob_sql = bucket.blob(sql_txt)
    blob_sql.upload_from_string('select * from `projeto.dataset.tabela')
    downloaded_sql = "select * from `projeto.dataset.tabela"

bql = bql.replace("replace_date_inicial", "'" + downloaded_data + "'")
bql = bql.replace("replace_date_final", "'" + timestamp + "'")

parameters = {"query": bql}
body = {
    "jobName": "{jobname}".format(jobname=jobname),
    "parameters": parameters
}
#
downloaded = downloaded_template.replace(downloaded_sql, bql)
print(downloaded)
# blob_template.upload_from_string(downloaded)
# blob_data.upload_from_string(timestamp)
# blob_sql.upload_from_string(bql)



