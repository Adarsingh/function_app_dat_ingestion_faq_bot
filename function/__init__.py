
import logging
import pandas as pd
import requests
import azure.functions as func
from io import StringIO
import os
import json
from azure.identity import DefaultAzureCredential

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing request to read CSV from Azure DevOps.')

    # Azure DevOps parameters
    organization = 'adarshs0791'
    project = 'FAQ_Copilot'
    repository = 'csv_files'
    file_path = 'main/'

    # Use Managed Identity to get an access token
    credential = DefaultAzureCredential()
    token = credential.get_token('https://dev.azure.com/.default').token

    local_directory = os.path.join(os.getcwd(), 'data')
    os.makedirs(local_directory, exist_ok=True)

    # Construct URL to access the file
    url = f'https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repository}/items?path={file_path}&version=GBmain&api-version=6.0'

    # Set headers with Managed Identity token
    headers = {
        'Authorization': f'Bearer {token}'
    }

    try:
        # Make request to Azure DevOps
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        files = response.json().get('value', [])

        # Filter out only CSV files
        csv_files = [file['path'] for file in files if file['path'].endswith('.csv')]
        logging.info(f'CSV files to process: {csv_files}')

        json_data = []

        # Process each CSV file
        for file_path in csv_files:
            # Construct URL to access each CSV file
            file_url = f'https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repository}/items?path={file_path}&version=GBmain&api-version=6.0'
            
            # Make request to get CSV file content
            file_response = requests.get(file_url, headers=headers)
            file_response.raise_for_status()
            
            # Read CSV content into a pandas DataFrame
            data = file_response.content
            df = pd.read_csv(StringIO(data.decode('utf-8')))
            
            # Display first few rows of DataFrame for debugging
            logging.info(f'Content of {file_path}:')
            logging.info(f'\n{df.head()}')

            # Save DataFrame to a local CSV file
            local_file_path = os.path.join(local_directory, os.path.basename(file_path))
            logging.info(f'Saving file to: {local_file_path}')
            df.to_csv(local_file_path, index=False)

            # Convert DataFrame to JSON
            json_data.extend(df.to_dict(orient='records'))
        
        return func.HttpResponse(json.dumps(json_data), mimetype="application/json")

    except Exception as e:
        logging.error(f'Error: {str(e)}')
        return func.HttpResponse(f'Error: {str(e)}', status_code=500)




# ===========================================================================================================================================================
# Using MAnaged identity.

# import logging
# import pandas as pd
# import requests
# import azure.functions as func
# from io import StringIO
# import os
# from azure.identity import DefaultAzureCredential

# def main(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Processing request to read CSV from Azure DevOps.')

#     # Azure DevOps parameters
#     organization = 'adarshs0791'
#     project = 'FAQ_Copilot'
#     repository = 'csv_files'
#     file_path = 'main/'
#     # pat = os.getenv('PAT')

#     # Use Managed Identity to get an access token
#     credential = DefaultAzureCredential()
#     token = credential.get_token('https://dev.azure.com/.default').token

#     local_directory = os.path.join(os.getcwd(), 'data')
#     os.makedirs(local_directory, exist_ok=True)

#     # Construct URL to access the file
#     url = f'https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repository}/items?path={file_path}&version=GBmain&api-version=6.0'

#     # Set headers with PAT for authentication
#     headers = {
#         'Authorization': f'Bearer {token}'
#     }

#     try:
#         # Make request to Azure DevOps
#         response = requests.get(url, headers=headers)
#         response.raise_for_status()
#         files = response.json().get('value', [])

#         # Filter out only CSV files
#         csv_files = [file['path'] for file in files if file['path'].endswith('.csv')]
#         logging.info(f'CSV files to process: {csv_files}')

#         json_data = []

#         # Process each CSV file
#         for file_path in csv_files:
#             # Construct URL to access each CSV file
#             file_url = f'https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repository}/items?path={file_path}&version=GBmain&api-version=6.0'
            
#             # Make request to get CSV file content
#             file_response = requests.get(file_url, headers=headers)
#             file_response.raise_for_status()
            
#             # Read CSV content into a pandas DataFrame
#             data = file_response.content
#             df = pd.read_csv(StringIO(data.decode('utf-8')))
            
#             # Display first few rows of DataFrame for debugging
#             logging.info(f'Content of {file_path}:')
#             logging.info(f'\n{df.head()}')

#             # Save DataFrame to a local CSV file
#             local_file_path = os.path.join(local_directory, os.path.basename(file_path))
#             logging.info(f'Saving file to: {local_file_path}')
#             df.to_csv(local_file_path, index=False)

#             # Convert DataFrame to JSON
#             json_data.extend(df.to_dict(orient='records'))
        
#         return func.HttpResponse(json.dumps(json_data), mimetype="application/json")

#         # return func.HttpResponse("CSV files successfully saved locally.", status_code=200)
    

#     except Exception as e:
#         logging.error(f'Error: {str(e)}')
#         return func.HttpResponse(f'Error: {str(e)}', status_code=500)


# =======================================================================================================================================
#Read from devops and write to local


# import logging
# import pandas as pd
# import requests
# import azure.functions as func
# from io import StringIO
# import os


# def main(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Processing request to read CSV from Azure DevOps.')

#     # Azure DevOps parameters
#     organization = 'adarshs0791'
#     project = 'FAQ_Copilot'
#     repository = 'csv_files'
#     file_path = 'main/'
#     pat = os.getenv('PAT')

#     local_directory = os.path.join(os.getcwd(), 'data')
#     os.makedirs(local_directory, exist_ok=True)

#     # Construct URL to access the file
#     url = f'https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repository}/items?path={file_path}&version=GBmain&api-version=6.0'

#     # Set headers with PAT for authentication
#     headers = {
#         'Authorization': f'Basic {pat}'
#     }

#     try:
#         # Make request to Azure DevOps
#         response = requests.get(url, headers=headers)
#         response.raise_for_status()
#         files = response.json().get('value', [])

#         # Filter out only CSV files
#         csv_files = [file['path'] for file in files if file['path'].endswith('.csv')]
#         logging.info(f'CSV files to process: {csv_files}')

#         # Process each CSV file
#         for file_path in csv_files:
#             # Construct URL to access each CSV file
#             file_url = f'https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repository}/items?path={file_path}&version=GBmain&api-version=6.0'
            
#             # Make request to get CSV file content
#             file_response = requests.get(file_url, headers=headers)
#             file_response.raise_for_status()
            
#             # Read CSV content into a pandas DataFrame
#             data = file_response.content
#             df = pd.read_csv(StringIO(data.decode('utf-8')))
            
#             # Save DataFrame to a local CSV file
#             local_file_path = os.path.join(local_directory, os.path.basename(file_path))
#             logging.info(f'Saving file to: {local_file_path}')
#             df.to_csv(local_file_path, index=False)

#             # Convert DataFrame to JSON
#         #     json_data = df.to_json(orient='records')
        
#         # return func.HttpResponse(json_data, mimetype="application/json")

#         return func.HttpResponse("CSV files successfully saved locally.", status_code=200)
    

#     except Exception as e:
#         logging.error(f'Error: {str(e)}')
#         return func.HttpResponse(f'Error: {str(e)}', status_code=500)


#=============================================================================================================================================================
#Initital code

# import logging
# import pandas as pd
# import requests
# import azure.functions as func
# from io import StringIO
# import os


# def main(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Processing request to read CSV from Azure DevOps.')

#     # Azure DevOps parameters
#     organization = 'adarshs0791'
#     project = 'FAQ_Copilot'
#     repository = 'csv_files'
#     # file_path = 'main/Admin_Branch.csv'
#     file_path = 'main/'
#     # pat = 'your_personal_access_token'
#     pat = os.getenv('PAT')

#     # Construct URL to access the file
#     url = f'https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repository}/items?path={file_path}&version=GBmain&api-version=6.0'

#     # Set headers with PAT for authentication
#     headers = {
#         'Authorization': f'Basic {pat}'
#     }

#     try:
#         # Make request to Azure DevOps
#         response = requests.get(url, headers=headers)
#         response.raise_for_status()

#         # Read CSV content into a pandas DataFrame
#         data = response.content
#         df = pd.read_csv(StringIO(data.decode('utf-8')))

#         #Write content file to csv file locally
#         local_file_path = '/tmp/admin_branch_data.csv'
#         df.to_csv(local_file_path, index=False)
        
#         # Convert DataFrame to JSON
#         json_data = df.to_json(orient='records')
        
#         return func.HttpResponse(json_data, mimetype="application/json")
#     except Exception as e:
#         logging.error(f'Error: {str(e)}')
#         return func.HttpResponse(f'Error: {str(e)}', status_code=500)