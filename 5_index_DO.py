#!/usr/bin/env python3


#
# find listed column that hasnt been index yet
# Index the column aa, bb, cc to improve mysql searching with '=' more faster like zzzuppppp
#


import mysql.connector
from mysql.connector.constants import ClientFlag

config = {
	'host' : '',
	'user' : '',
	'password' : '',
	'port' : ,
	'database' : ''
}

def not_index_list():

	try:
		konet = mysql.connector.connect(**config)
		run = konet.cursor()

		query = """
		SELECT      column_name, table_name, column_key
FROM        INFORMATION_SCHEMA.COLUMNS
WHERE       (column_name LIKE 'aa' or column_name LIKE 'bb' or column_name LIKE 'cc') and table_schema = 'ab' and column_key != 'MUL'
ORDER BY    table_name, column_name, column_key;
""".strip()

		run.execute(query)
		semua = run.fetchall()
		return semua

	except mysql.connector.errors.ProgrammingError as error:
		print(error)

def change_type(table, column):

	try:
		konet = mysql.connector.connect(**config)
		run = konet.cursor()

		query = f"""
			ALTER TABLE ab.{table} MODIFY COLUMN {column} VARCHAR (255);
		""".strip()

		print(query)

		run.execute(query)
		konet.commit()

		print(run.rowcount, " record(s) affected")
		print(f"Done change column type = {column} VARCHAR (255)\n")

	except mysql.connector.errors.ProgrammingError as error:
		print(error)

def create_index(table, column):
	
	try:
		konet = mysql.connector.connect(**config)
		run = konet.cursor()

		query = f"""
			alter table ab.{table} add index({column})
		""".strip()

		print(query)

		run.execute(query)
		konet.commit()

		print(run.rowcount, " record(s) affected")
		print(f"Done add index {column} key to ab.{table}\n")

	except mysql.connector.errors.ProgrammingError as error:
		print(error)


def main():

	tables = not_index_list()

	meja = ''

	for i in tables:
		print(f'{i[0]} : {i[1]}')

		# if i[1] != meja:
		# 	meja = i[1]
		
		change_type(i[1], i[0])

		create_index(i[1], i[0])




if __name__ == '__main__':
	main()
else:
	print('imported')