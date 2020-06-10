from googleapiclient.discovery import build
import google.auth
from google.cloud import storage
from datetime import datetime, timedelta
import pytz


def function(request):
    try:
        credentials, project_id = google.auth.default()
        service = build('dataflow', 'v1b3', credentials=credentials, cache_discovery=False)

        # parametros
        bql = "query WHERE DATE(campo) >= replace_date"
        bq_table_source = bql.split('`')[1].split('.')
        bucket_name = "bucket"
        jobname = "namejob"
        template = "teste/templates/teste"

        #data & query
        data_txt = '{template}/data.txt'.format(template=template)
        sql_txt = '{template}/sql.txt'.format(template=template)        
        gcsPath = "gs://bucket/{template}".format(template=template)

        # parametros para validações não mexer
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        stats = storage.Blob(bucket=bucket, name=data_txt).exists(storage_client)
        stats_sql = storage.Blob(bucket=bucket, name=sql_txt).exists(storage_client)
        bucket = storage_client.get_bucket(bucket_name)

        # olha a hora atual para salvar  e converte pra string
        now = datetime.now(pytz.timezone("Brazil/East"))
        timestamp = now.strftime("%Y-%m-%d")

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
            blob_data.upload_from_string('1900-01-01')
            downloaded_data = "1900-01-01"

        if (stats_sql == True):
            # read data from arquivo
            bucket = storage_client.get_bucket(bucket_name)
            blob_sql = bucket.get_blob(sql_txt)
            downloaded_sql = blob_sql.download_as_string()
            downloaded_sql = downloaded_sql.decode('utf-8')
        else:
            bucket = storage_client.get_bucket(bucket_name)
            blob_sql = bucket.blob(sql_txt)
            blob_sql.upload_from_string('select * from projeto.dataset.tabela')
            downloaded_sql = "select * from projeto.dataset.tabela"

        bql = bql.replace("replace_date", "'" + downloaded_data + "'")
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

