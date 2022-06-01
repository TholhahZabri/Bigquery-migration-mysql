from google.cloud import bigquery

client =  bigquery.Client.from_service_account_json('auth.json')
bucket_name = 'gs://raw.raw.onl/test/' 
project = 'ab_proj'
dataset_id = 'ab_ds'

def main():

############## Feed table automatically #################### 
   
    # tables = client.list_tables(dataset_id)

    # for table_id in tables:
    #     destination_uri = bucket_name + f'/{table_id.table_id}/{table_id.table_id}*'
    #     print(destination_uri)

        # dataset_ref = bigquery.DatasetReference(project, dataset_id)
        # table_ref = dataset_ref.table(table_id)

        # extract_job = client.extract_table(
        #     table_ref,
        #     destination_uri,
        #     # Location must match that of the source table.
            # location="asia-southeast1",
        # )  # API request
        # extract_job.result()  # Waits for job to complete.

        # print(
        #     "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
        # )


############## Feed table manually #################### 

    # # table_id = ''
    # # table_id = ''
    # table_id = ''

    # dataset_ref = bigquery.DatasetReference(project, dataset_id)
    # table_ref = dataset_ref.table(table_id)
    # destination_uri = bucket_name + f'/{table_id}/{table_id}*'

    # export_job = client.extract_table(
    #         table_ref,
    #         destination_uri,
    #         location = 'asia-southeast1',
    #     )
    
    # export_job.result()
    # print(f"exported {table_id} to {destination_uri}")


############## Feed table by list text #################### 
    
    redo = open('total_table_migrate_redo', 'r')
    for line in redo:
        # print(line.strip())

        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_id = line.strip()
        table_ref = dataset_ref.table(table_id)
        destination_uri = bucket_name + f'/{table_id}/{table_id}*'

        export_job = client.extract_table(
                table_ref,
                destination_uri,
                location = 'asia-southeast1',
            )
        
        export_job.result()
        print(f"exported {table_id} to {destination_uri}")

        print('\n')


if __name__ == '__main__':
    main()