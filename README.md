# BigqueryToEventHub

Baixar o repositório do GitHub. 

Abrir com alguma IDE (PYCHARM) o repositório.  

No terminal iremos executar o comando abaixo para criar um template no storage do google para que possamos schedular. 

# EVENTHUB 

python -m BigQueryToEventHub  
--bql "QUERY"   
--project ID_PROJETO   
--job_name NOME_DATAFLOW   
--runner DataflowRunner  
--setup_file DIRETORIO_ONDE_ESTÁ_REPOSITORIO/setup.py   
--requirements_file requirements.txt (bibliotecas que iremos utilizar) 
--autoscaling_algorithm=THROUGHPUT_BASED  
--max_num_workers=3 (NÚMERO DE MÁQUINAS QUE IRÁ EXECUTAR O DATAFLOW)   
--machine_type=n1-standard-2 (TIPO DA MÁQUINA QUE IRÁ EXECUTAR O DATAFLOW)  
--connection_str  "STRING DE CONEXÃO COM EVENTHUB"  
--eventhub_name "NOME_EVENT_HUB" 
--template_location LOCAL_ONDE_IRÁ_CRIAR_TEMPLATE  
--temp_location  LOCAL_ONDE_IRÁ_CRIAR_TEMPLATE  
--bucket NOME_DO_BUCKET 

# EXEMPLO DE SQL  
(incremental) 

SELECT   *, created_at 
FROM       `projeto.dataset.tabela` 
Where format_timestamp('%Y-%m-%d%H:%M:%S', (created_at)) < replace_date_final 
and  format_timestamp('%Y-%m-%d%H:%M:%S', (created_at)) < replace_date_final 

# Gcloud Function 

from googleapiclient.discovery import build 
import google.auth 
from google.cloud import storage 
from datetime import datetime 
import pytz   

def clean_deliveryline(request): 
    try: 
        credentials, project_id = google.auth.default() 
        service = build('dataflow', 'v1b3', credentials=credentials, cache_discovery=False)   

        # parametros 
        bql = QUERY" 
        bq_table_source = bql.split('`')[1].split('.') 
        data_txt = 'caminho/data.txt'

        sql_txt = 'caminho/sql.txt' 
        template = "caminho" 
        bucket_name = "bucket" 
        jobname = "NAME_JOB" 
        gcsPath = "path_template"   

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
            blob_data.upload_from_string('1900-01-01 00:00:00') 
            downloaded_data = "1900-01-01 00:00:00"   

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
        downloaded = downloaded_template.replace(downloaded_sql, bql) 
        blob_template.upload_from_string(downloaded) 
        blob_data.upload_from_string(timestamp) 
        blob_sql.upload_from_string(bql) 
        request = service.projects().templates().launch(projectId=project_id, gcsPath=gcsPath, body=body) 
        response = request.execute()   

    except: 
        blob_template.upload_from_string(downloaded_template) 
        blob_data.upload_from_string(downloaded_data) 
        blob_sql.upload_from_string(downloaded_sql) 

 
