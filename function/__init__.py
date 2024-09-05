
import os
import json
import azure.functions as func
from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication
from azure.devops.v7_1.git.models import GitVersionDescriptor
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential


def create_blob_connection(container_name):

    print("Connecting to Azure Blob Storage using Managed Identity...")
    try:
        # Use DefaultAzureCredential to authenticate
        credential = DefaultAzureCredential()
        
        # Retrieve account name from environment
        account_name = 'dataingestionpocblob'
        
        # Construct the endpoint URL
        endpoint = f'https://{account_name}.blob.core.windows.net'

        # Initialize BlobServiceClient
        blob_service_client = BlobServiceClient(account_url=endpoint, credential=credential)

        # Create a container client
        container_client = blob_service_client.get_container_client(container_name)

        # Ensure the container exists
        if not container_client.exists():
            container_client.create_container()

        print("Returning container_client")

        return container_client
    
    except Exception as e:
        print(f"An error occurred while connecting to blob storage: {e}")
        raise


def upload_file_to_blob(blob_client, file_content, filename):
    try:
        print(f"Uploading '{filename}'...")
        blob_client.upload_blob(file_content, overwrite=True)
        print(f"Successfully uploaded '{filename}' to container.")
    except Exception as e:
        print(f"Failed to upload '{filename}': {e}")
        raise


def transfer_files_from_devops_to_blob(pat, base_url, project, repository, branch_name, file_path, container_client):

    credentials = BasicAuthentication('', pat)
    connection = Connection(base_url=base_url, creds=credentials)

    try:
        git_client = connection.clients.get_git_client()
        items = git_client.get_items(
            repository_id=repository,
            project=project,
            scope_path=file_path,
            version_descriptor=GitVersionDescriptor(
                version=branch_name,
                version_type='branch'
            ),
            recursion_level='Full'
        )

        files_list = [item.path for item in items if item.git_object_type == 'blob' and (item.path.endswith('.pdf') or item.path.endswith('.csv'))]

        if not files_list:
            print("No PDF or CSV files found in the repository.")
            return  # Just return; no need to send an HTTP response here

        for file in files_list:
            item_content = git_client.get_item_content(
                repository_id=repository,
                project=project,
                path=file,
                version_descriptor=GitVersionDescriptor(
                    version=branch_name,
                    version_type='branch'
                )
            )

            content = b''.join(item_content) if hasattr(item_content, '__iter__') else item_content

            # Prepare blob client
            blob_filename = os.path.basename(file)
            blob_client = container_client.get_blob_client(blob_filename)

            # Upload directly to blob storage
            upload_file_to_blob(blob_client, content, blob_filename)
        
    except Exception as e:
        print(f"An error occurred: {e}")
        raise


def main(req: func.HttpRequest) -> func.HttpResponse:
    organization = 'adarshs0791'
    project = 'FAQ_Copilot'
    repository = 'csv_files' 
    pat = os.getenv('PAT')
    branch_name = 'main'  
    file_path = ''  
    # container_name = 'devops-ingestion-container'
    container_name = 'data-ingestion-poc-container'
    base_url = f'https://dev.azure.com/{organization}'
    
    container_client = create_blob_connection(container_name)

    try:
        print("Starting to transfer files...")
        transfer_files_from_devops_to_blob(pat, base_url, project, repository, branch_name, file_path, container_client)
        print("All files have been transfered successfully.")
    except Exception as e:
        print(f"An error occurred while transfering files: {e}")
        return func.HttpResponse(f"An error occurred while transfering files: {e}", status_code=500)

    return func.HttpResponse("Process completed successfully.", status_code=200)


# --------------------------------------------------------------------------------------------------------------------------------------------------
# Final Code
#Get files (csv, pdf) from devops
#Upload it to blob directly.

# import os
# import json
# import azure.functions as func
# from azure.devops.connection import Connection
# from msrest.authentication import BasicAuthentication
# from azure.devops.v7_1.git.models import GitVersionDescriptor
# from azure.storage.blob import BlobServiceClient


# def create_blob_connection(container_name):

#     print("Connecting to Azure Blob Storage...")

#     try:
#         # Retrieve account name and key from environment variables
#         account_name = os.getenv('StorageAccountName')
#         account_key = os.getenv('StorageAccountKey')

#         # Construct the connection string
#         connection_string = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'

#         # Initialize BlobServiceClient
#         blob_service_client = BlobServiceClient.from_connection_string(connection_string)

#         # Create a container client
#         container_client = blob_service_client.get_container_client(container_name)

#         # Ensure the container exists
#         if not container_client.exists():
#             container_client.create_container()

#         print("Returning container_client")

#         return container_client

#     except Exception as e:
#         print(f"An error occurred while connecting to blob storage: {e}")
#         raise


# def upload_file_to_blob(blob_client, file_content, filename):
#     try:
#         print(f"Uploading '{filename}'...")
#         blob_client.upload_blob(file_content, overwrite=True)
#         print(f"Successfully uploaded '{filename}' to container.")
#     except Exception as e:
#         print(f"Failed to upload '{filename}': {e}")
#         raise


# def transfer_files_from_devops_to_blob(pat, base_url, project, repository, branch_name, file_path, container_client):

#     credentials = BasicAuthentication('', pat)
#     connection = Connection(base_url=base_url, creds=credentials)

#     try:
#         git_client = connection.clients.get_git_client()
#         items = git_client.get_items(
#             repository_id=repository,
#             project=project,
#             scope_path=file_path,
#             version_descriptor=GitVersionDescriptor(
#                 version=branch_name,
#                 version_type='branch'
#             ),
#             recursion_level='Full'
#         )

#         files_list = [item.path for item in items if item.git_object_type == 'blob' and (item.path.endswith('.pdf') or item.path.endswith('.csv'))]

#         if not files_list:
#             print("No PDF or CSV files found in the repository.")
#             return  # Just return; no need to send an HTTP response here

#         for file in files_list:
#             item_content = git_client.get_item_content(
#                 repository_id=repository,
#                 project=project,
#                 path=file,
#                 version_descriptor=GitVersionDescriptor(
#                     version=branch_name,
#                     version_type='branch'
#                 )
#             )

#             content = b''.join(item_content) if hasattr(item_content, '__iter__') else item_content

#             # Prepare blob client
#             blob_filename = os.path.basename(file)
#             blob_client = container_client.get_blob_client(blob_filename)

#             # Upload directly to blob storage
#             upload_file_to_blob(blob_client, content, blob_filename)
        
#     except Exception as e:
#         print(f"An error occurred: {e}")
#         raise


# def main(req: func.HttpRequest) -> func.HttpResponse:
#     organization = 'adarshs0791'
#     project = 'FAQ_Copilot'
#     repository = 'csv_files' 
#     pat = os.getenv('PAT')
#     branch_name = 'main'  
#     file_path = ''  
#     # container_name = 'devops-ingestion-container'
#     container_name = 'data-ingestion-poc-container'
#     base_url = f'https://dev.azure.com/{organization}'
    
#     container_client = create_blob_connection(container_name)

#     try:
#         print("Starting to transfer files...")
#         transfer_files_from_devops_to_blob(pat, base_url, project, repository, branch_name, file_path, container_client)
#         print("All files have been transfered successfully.")
#     except Exception as e:
#         print(f"An error occurred while transfering files: {e}")
#         return func.HttpResponse(f"An error occurred while transfering files: {e}", status_code=500)

#     return func.HttpResponse("Process completed successfully.", status_code=200)


# --------------------------------------------------------------------------------------------------------------------------------------------------
