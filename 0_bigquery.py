#!/usr/env/bin python3

#
#
# Compare table in BQ and in MySQL for increment update purpose
#
#

from google.cloud import bigquery
import mysql.connector

from mysql.connector.constants import ClientFlag

project = 'ab_proj'
dataset_id = 'ab_ds'

client = bigquery.Client.from_service_account_json('auth.json')

config1 = {
	'host' : '',
	'user' : '',
	'password' : '',
	'port' : ,
	'database' : 'ab'
}

## gcloud config
config2 = {
    'user': '',
    'password': '',
    'host': '',
    'client_flags': [ClientFlag.SSL],
    'ssl_ca': './server-ca.pem',
    'ssl_cert': './client-cert.pem',
    'ssl_key': './client-key.pem',
    'database': 'ab'
}

def mysql_count(table, config):

	query = f"""
		SELECT count(*) from ab.{table.table_id};
	""".strip()

	# query = f"""
	# 	SELECT count(*) from ab.{table};
	# """.strip()

	try:
		konet = mysql.connector.connect(**config)
		run = konet.cursor()
		# print(query, sep='', end='\t\t')

		run.execute(query)
		# return run.fetchall()
		semua = run.fetchall()

		for data in semua:
			print(data[0])
	except mysql.connector.errors.ProgrammingError as error:
		print('error')

def bq_count(table):
	query = f"""
		SELECT count(*) from {table.project}.{table.dataset_id}.{table.table_id};
	""".strip()
	# print(query, sep='', end='\t\t')
	query_job = client.query(query)
	for row in query_job:
		print(row[0])

def gcloud_sql_count(table):
	query = f"""
		SELECT count(*) from ab.{table.table_id};
	""".strip()

	try:
		konet = mysql.connector.connect(**config2)
		run = konet.cursor()
		# print(query, sep='', end='\t\t')

		run.execute(query)
		# return run.fetchall()
		semua = run.fetchall()

		for data in semua:
			print(data[0])
	except mysql.connector.errors.ProgrammingError as error:
		print(error)

def mysql_new(table, config):
	query = f"""
		SELECT count(*) from ab.{table};
	""".strip()

	try:
		konet = mysql.connector.connect(**config)
		run = konet.cursor()
		# print(query, sep='', end='\t\t')

		run.execute(query)
		# return run.fetchall()
		semua = run.fetchall()

		for data in semua:
			print(data[0])
	except mysql.connector.errors.ProgrammingError as error:
		print(error)


def query():


	### list tables
	tables = client.list_tables(dataset_id)

	for table in tables:
		# print(type(table))
		# for listItem in table:
		# 		print(listItem)
		full_table = f'{table.project}.{table.dataset_id}.{table.table_id}'
		if full_table == 'ab_table' or full_table == 'ab_table_new':
			pass
		else:
			print(f'{table.project}.{table.dataset_id}.{table.table_id}')

		# print(f'{table.project}.{table.dataset_id}.{table.table_id}')

		bq_count(table)
		
		mysql_count(table, config1)

		# print(f"table {table}.....")

		# gcloud_sql_count(table)

		# ### or ###
		# lines = open('total_table_migrate_redo', 'r')

		# for line in lines:
		# 	table = line.strip()

		# 	print(f"table = {table}....")
		# 	mysql_new(table, config1)
		# 	mysql_new(table, config2)




if __name__ == '__main__':
	query()
else:
	print('imported')