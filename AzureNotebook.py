blob_account_name = "projectstorage03"
blob_container_name = "container01"
blob_relative_path = "Days and hours of work in Old and New Worlds - Huberman and Minns (2007).csv"
blob_sas_token = r"?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-11-11T08:55:52Z&st=2024-11-11T00:55:52Z&spr=https&sig=jKLtEdk9plICgQmwEs%2BAMuv9ZuqjQHDt4w7uSYMgRFA%3D"


wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)
print('Remote blob path: ' + wasbs_path)


df = spark.read.csv(wasbs_path, header=True, inferSchema=True)
print('Register the DataFrame as a SQL temporary view: source')
df.createOrReplaceTempView('source')


df.count()

%sql
SELECT * FROM source LIMIT 0

%sql
Select DISTINCT Entity from source
