#!/usr/bin/env python3

from google.cloud import storage

import mysql.connector
from mysql.connector.constants import ClientFlag


## gcloud config
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

def get_tables():
	konet = mysql.connector.connect(**config)
	run = konet.cursor()

	query = "show tables;"
	print(query)

	run.execute(query)
	return run.fetchall()


# run sql to fetch output
def run_sql_fetch(table):
	konet = mysql.connector.connect(**config)
	run = konet.cursor()

	query = f"select column_name from information_schema.columns where table_schema = database() and table_name = '{table}'"
	print(query)

	run.execute(query)
	return run.fetchall()


# run sql to delete entries same as column name during ingestion
def run_sql_delete(table, i):
	konet = mysql.connector.connect(**config)
	run = konet.cursor()

	query = f"delete from {table} where {i} = '{i}'"
	print(query)

	run.execute(query)
	konet.commit()
	print(run.rowcount, " records(s) affected")


def main():


	tables = get_tables()
	for table in tables:
		# print(table[0])
		result = run_sql_fetch(table[0])

		for i in result:
			# print(i[0])
			# print(result)
			run_sql_delete(table[0], i[0])
			
			break
		# break



if __name__ == '__main__':
	main()
else:
	print('imported')