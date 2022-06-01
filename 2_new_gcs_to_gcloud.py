#!/usr/bin/env python3

from google.cloud import storage
from google.cloud import bigquery
import os

import mysql.connector
from mysql.connector.constants import ClientFlag

import subprocess

# gcloud config
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

	redo = open('total_table_migrate_redo', 'r')

	for line in redo:
		table_id = line.strip()

		# Create table schema full aaa.bbb.ccc
		table_full = f'{project}.{dataset_id}.{table_id}'
		table = client.get_table(table_full)
		results = ["{0}".format(schema.name) for schema in table.schema]

		# Create SQL create table statement
		query = "CREATE TABLE IF NOT EXISTS {} (".format(table_id)
		awal = 0
		for result in results:
			# if result.casefold() == "no_no".casefold():
			#	 query += " varchar(255) NOT NULL, "
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
		command = f"gsutil ls gs://raw.raw.onl/test/{table_id}/*000*"
		# print(command)

		output = os.popen(command).read().splitlines()
		for out in output:
			command = "gcloud sql import csv mmbg " + out + " -d ab --table=" + table_id + " --quiet"
			print(command)
			print(os.popen(command).read())

		print('\n')


if __name__ == '__main__':
	main()
else:
	print('imported')