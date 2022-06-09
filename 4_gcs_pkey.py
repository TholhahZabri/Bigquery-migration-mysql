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

def add_pkey(table):
    konet = mysql.connector.connect(**config)
    run = konet.cursor()

    query = f"alter table ab.{table} ADD PKID INT AUTO_INCREMENT PRIMARY KEY FIRST;"
    run.execute(query)
    konet.commit()
    
    print(run.rowcount, " record(s) affected")
    print(f"Done add primary key to ab.{table}\n")


def main():


    tables = get_tables()
    for table in tables:
        # print(table[0])
        result = add_pkey(table[0])

        

if __name__ == '__main__':
    main()
else:
    print('imported')