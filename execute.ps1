#!/usr/bin/pwsh
#C:\path\to\directory\Scripts\activate.ps1

# login on gcp
# gcloud init && gcloud auth application-default login

# tracking origin data
#export PROJECT_ORIGIN=post-external-api-full

# tracking resources on gcp
#export GOOGLE_APPLICATION_CREDENTIALS=/home/${USER}/.config/gcloud/application_default_credentials.json

echo "==================================================================="
echo "Script de criacao dos arquivos de configuracao e execucao dos ETLs"
echo "==================================================================="
echo ""
echo "(Databricks) Digite o nome do job"
[string] $name_job = $( Read-Host "job" )
echo ""
echo "(Databricks) Digite o(s) campo(s) chave separados por (|)"
[string] $merge = $( Read-Host "PK (|)" )
echo ""
echo "(Databricks) Digite o campo de particao(DATE)"
[string] $partition = $( Read-Host "particao(DATE)" )
echo ""
echo "(Databricks) Digite o nome da database"
[string] $db_name = $( Read-Host "database" )
echo ""
echo "(Databricks) Digite o nome da tabela"
[string] $tbName = $( Read-Host "tabela" )
echo ""
echo "(Databricks) Digite SasKey do (LISTEN) do EH"
[string] $sasKey = $( Read-Host "SasKey do (LISTEN)" )
echo ""
echo "(Databricks) Digite a connection_str do EventHub (LISTEN):"
[string]$connection_str_listen = $( Read-Host "connection_str (LISTEN)" )
echo ""
echo "(Google) Digite a connection_str do EventHub (SEND):"
[string]$connection_str = $( Read-Host "connection_str (SEND)" )
echo ""
echo "(Google) Digite a EventHub Name (Topic) (SEND):"
[string]$eh_name = $( Read-Host "EventHub (SEND)" )
echo ""
echo "(Google) Digite o nome da function"
[string] $namefunction = $( Read-Host "function" )
echo ""
echo "(Google) Digite a Bucket Location exemplo = gs://bucket/bucket"
[string]$location = $( Read-Host "Location" )
echo ""
echo ""
$base_parameters = $('{\"dbName\":\"'+ ($db_name) +'\",\"tbName\":\"'+ ($tbName)+'\",\"event_hub_conn_string\":\"'+ ($connection_str_listen)+'\",\"saskey\":\"'+ ($sasKey)+'\",\"merge\":\"'+($merge)+'\",\"datepartition\":\"'+($partition)+'\",\"eh_name\":\"'+($eh_name)+'\"}')
$notebook_task =  $('{\"notebook_path\":\"/teste/teste/teste/teste\",\"base_parameters\":' + $base_parameters + ',\"revision_timestamp\":0}')
echo ""
echo "==================================================================="
echo "                   Script Template para o storage                  "
echo "==================================================================="
echo ""
python -m BigQueryToEventHub --query "select * from `projeto.dataset.tabela" --project teste --region "us-central1" --runner DataflowRunner --setup_file /path/to/directory/setup.py --requirements_file requirements.txt --autoscaling_algorithm=THROUGHPUT_BASED --max_num_workers=1 --machine_type=n1-standard-2 --connection_str ($connection_str) --eventhub_name ($eh_name) --template_location ($location) --temp_location ($location)
echo ""
echo "==================================================================="
echo "                   Script Upload Function                          "
echo "==================================================================="
echo ""
gcloud functions deploy ($namefunction) --project teste --entry-point "function" --region "us-central1" --runtime python37 --trigger-http --allow-unauthenticated --source="C:\path\to\directory\function"
echo ""
echo "==================================================================="
echo "                   Criação job Databricks                          "
echo "==================================================================="
echo ""
$jobs_list=$(databricks jobs list --profile Steve | Select-String "notebook_copy" | %{$_ -split"  "}) 
foreach ($j in $jobs_list[0])
{
  [string] $settings=$(databricks jobs get --job-id $j --profile Steve | jq -c ".settings" | jq -c --arg name_job "$name_job"'.name = ($name_job)' | jq -c --arg notebook_task "$notebook_task"'.notebook_task = ($notebook_task)')
  $result = $($settings.Replace('"','\"').Replace('\\"','\"').Replace('\"{\','{\').Replace('}\"','}'))
  echo($result)
  $(databricks jobs create --json '"'($result)'"' --profile Steve)
}