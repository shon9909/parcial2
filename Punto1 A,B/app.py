import boto3
import requests
import time
#Nombre del bucket en s3
bucket="parcial21"
def handler(event,context):
	#Se crea una variable con el tiempo actual
	fecha=time.localtime()
	s3 = boto3.resource('s3')
	#Se llama la funcion crearPath para enviar los datos necesarios
	crearPath("Avianca","https://query1.finance.yahoo.com/v7/finance/download/AVHOQ?period1=1634601600&period2=1634688000&interval=1d&events=history&includeAdjustedClose=true",fecha,bucket,s3)
	crearPath("Ecopetrol","https://query1.finance.yahoo.com/v7/finance/download/EC?period1=1634774400&period2=1634860800&interval=1d&events=history&includeAdjustedClose=true",fecha,bucket,s3)
	crearPath("GrupoAval","https://query1.finance.yahoo.com/v7/finance/download/AVAL?period1=1634774400&period2=1634860800&interval=1d&events=history&includeAdjustedClose=true",fecha,bucket,s3)
	crearPath("CementosArgos","https://query1.finance.yahoo.com/v7/finance/download/CMTOY?period1=1634774400&period2=1634860800&interval=1d&events=history&includeAdjustedClose=true",fecha,bucket,s3)
	return {
			"status_code":200
		}
#Funcion crearPath encargada de hacer la actualizacion en el bucket con los datos:
#Compañia, url, el nombre del bucket y resource s3
def crearPath(company,url,fecha,bucketname,s3):	
	headers = {'User-Agent': 'Mozilla'}
	r = requests.get(url, headers=headers)
	filepath="/tmp/"+company+".csv"
	f = open(filepath,"w")
	print("Guardando..")
	f.write(r.text)
	f.close()
	path='stocks/company='+company+'/year='+str(fecha.tm_year)+'/month='+str(fecha.tm_mon)+'/day='+str(fecha.tm_mday)+'/'+str(fecha.tm_hour)+str(fecha.tm_min)+str(fecha.tm_sec)+'page.csv'
	s3.meta.client.upload_file(filepath,bucketname, path)