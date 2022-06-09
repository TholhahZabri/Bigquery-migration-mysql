from google.cloud import storage
from google.cloud import bigquery
import os
import re

import mysql.connector
from mysql.connector.constants import ClientFlag

import subprocess

config = {
    'user': '',
    'password': '',
    'host': '',
    'client_flags': [ClientFlag.SSL],
    'ssl_ca': './server-ca.pem',
    'ssl_cert': './client-cert.pem',
    'ssl_key': './client-key.pem',
    'database': 'ab'
}

client =  bigquery.Client.from_service_account_json('auth.json')

def main():

	project = 'ab_proj'
	dataset_id = 'ab_ds'

	list_db_folder = open('./gcs_to_gcloud','r')

	for db in list_db_folder:
		
		list_db = re.findall("gs://raw.raw.onl/test/(.+)/", db)
		# print(list_db[0])
		table_id = list_db[0]

		# Create table schema full aaa.bbb.ccc
		table_full = "{}.{}.{}".format(project, dataset_id, table_id)
		table = client.get_table(table_full)
		results = ["{0}".format(schema.name) for schema in table.schema]

		# Create SQL create table statement
		query = "CREATE TABLE {} (".format(table_id)
		awal = 0
		for result in results:
			# if result.casefold() == "no_no".casefold():
			#	 query += "no_no varchar(255) NOT NULL, "
			# else:
			#	 query += "{} TEXT NULL, ".format(result)
			if awal == 0:
				query += "{} TEXT NULL".format(result)
				awal+=1
			else:
				query += ", {} TEXT NULL".format(result)
			
		query += ");"
		print(query)
		print()

		# now we establish our connection
		cnxn = mysql.connector.connect(**config)

		cursor = cnxn.cursor()
		cursor.execute(query)
		cnxn.close()

		# # using gsutil to list all files inside to import to gcloud
		command = "gsutil ls " + db.strip() +"*000*"
		# print(command)

		output = os.popen(command).read().splitlines()
		for out in output:
			command = "gcloud sql import csv mmbg " + out + " -d ab --table=" + table_id + " --quiet"
			print(command)
			print(os.popen(command).read())

		print('\n')
		break




if __name__ == '__main__':
	main()