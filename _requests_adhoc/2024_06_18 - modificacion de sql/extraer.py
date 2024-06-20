import warnings
import tableauserverclient as TSC

warnings.filterwarnings('ignore')

def update_custom_sql(data_source, server, old_table, new_table):
    try:
        # Get the connection information
        for connection in data_source.connections:
            if connection.connection_type == 'sql':
                # Retrieve the current custom SQL query
                custom_sql = connection.custom_sql
                print(custom_sql[:100])  # Print the first 100 characters for verification
                # Replace the table name in the custom SQL query
                updated_sql = custom_sql.replace(old_table, new_table)
                connection.custom_sql = updated_sql
        
        # Update the data source on the server
        server.datasources.update(data_source)
        print(f"Successfully updated the SQL query in the data source {data_source.name}.")
    except Exception as e:
        print(f"Failed to update the SQL query: {str(e)}")

def main():
    tableau_auth = TSC.PersonalAccessTokenAuth(
        'mi_token',
        '3RoPL/9HRYawGVlfd+eA9g==:IQ6l4lBqwurzkgWfUdWvGuMwumiKcDRL',
        'globalizationpartners'
    )
    server = TSC.Server('https://us-east-1.online.tableau.com', use_server_version=True)
    server.add_http_options({'verify': False})
    
    with server.auth.sign_in(tableau_auth):
        server.version = '3.5'
        
        # Fetch the data source you want to update
        all_datasources, pagination_item = server.datasources.get()
        for datasource in all_datasources:
            if datasource.name == 'MAs Over Time PDS':
                data_source_id = datasource.id
                break
        else:
            print("Data source 'MAs Over Time PDS' not found.")
            return
        
        # Fetch the data source by id to get full details
        data_source = server.datasources.get_by_id(data_source_id)
        print(f'se encontro {data_source.name}')
        
        # Populate connections for the data source
        server.datasources.populate_connections(data_source)
        
        # Update the custom SQL query
        update_custom_sql(data_source, server, 'gp_prod.gold_customer.customer_entity_view', 'gp_prod.gold_customer.customer_entity')

if __name__=='__main__':
    main()
