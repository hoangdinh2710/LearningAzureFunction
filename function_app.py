import json
import logging
import os

import azure.functions as func

from shared.azure_credential import (
    get_azure_default_credential,
    get_azure_key_credential,
)
from shared.bing_search import get_news
from shared.blob_storage import upload_to_blob
from shared.hash import get_random_hash
from shared.key_vault_secret import get_key_vault_secret
from shared.azure_credential import get_azure_default_credential
from shared.data_lake import upload_to_data_lake
from shared.transform import clean_documents

app = func.FunctionApp()

# Learn more at aka.ms/pythonprogrammingmodel

# Get started by running the following code to create a function using a HTTP trigger.

@app.function_name(name="SearchAndSaveResultToStorage")
@app.route(route="Hello")
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python http trigger function")

    # Get the query parameters
    search_term = req.params.get("search_term", "Quantum Computing")
    count = req.params.get("count", 10)

    # Get environment variables
    key_vault_resource_name = os.environ["KEY_VAULT_RESOURCE_NAME"]
    bing_secret_name = os.environ["KEY_VAULT_SECRET_NAME"]
    bing_news_search_url = os.environ["BING_SEARCH_URL"]
    blob_account_name = os.environ["BLOB_STORAGE_RESOURCE_NAME"]
    blob_container_name = os.environ["BLOB_STORAGE_CONTAINER_NAME"]

    # # Get authentication to Key Vault with environment variables
    azure_default_credential = get_azure_default_credential()

    # # Get the secret from Key Vault
    bing_key = get_key_vault_secret(
         azure_default_credential, key_vault_resource_name, bing_secret_name
    )

    # Get authentication to Bing Search with Key
    azure_key_credential = get_azure_key_credential(bing_key)

    # Get the search term:
    search_term = req.params.get('name','ChatGPT')

    # Clean up file name
    random_hash = get_random_hash()
    filename = f"search_results_{search_term}_{random_hash}.json".replace(" ", "_").replace(
        "-", "_"
    )


    # Get the search results
    news_search_results = get_news(azure_key_credential, bing_news_search_url, search_term, count)

    # Convert the result to JSON and save it to Azure Blob Storage
    if news_search_results.value:
        news_item_count = len(news_search_results.value)
        logging.info("news item count: %d", news_item_count)
        json_items = json.dumps([news.as_dict() for news in news_search_results.value])

        blob_url = upload_to_blob(
            azure_default_credential,
            blob_account_name,
            blob_container_name,
            filename,
            json_items,
        )
        logging.info("news uploaded: %s", blob_url)

    return filename


@app.function_name(name="BlobTrigger")
@app.blob_trigger(arg_name="myblob", path="samples-workitems/{name}",
                  connection="")

def main(myblob: func.InputStream):

    logging.info("Python blob trigger function processed blob \nName: %s \nBlob Size: %s bytes", myblob.name, myblob.length)

    # read the blob content as a string.
    search_results_blob_str = myblob.read()

    # decode the string to Unicode
    blob_json = search_results_blob_str.decode("utf-8")

    # parse a valid JSON string and convert it into a Python dict
    try:

        # Get environment variables
        data_lake_account_name = os.environ.get("DATALAKE_GEN_2_RESOURCE_NAME")
        data_lake_container_name = os.environ.get("DATALAKE_GEN_2_CONTAINER_NAME")
        data_lake_directory_name = os.environ.get("DATALAKE_GEN_2_DIRECTORY_NAME")

        # Get Data
        data = json.loads(blob_json)

        # Clean Data
        new_data_dictionary = clean_documents(data)

        # Prepare to upload
        json_str = json.dumps(new_data_dictionary)
        file_name = myblob.name.split("/")[1]
        new_file_name = f"processed_{file_name}"

        # Get authentication to Azure
        azure_default_credential = get_azure_default_credential()

        # Upload to Data Lake
        upload_to_data_lake(azure_default_credential, data_lake_account_name, data_lake_container_name, data_lake_directory_name, new_file_name, json_str)
        logging.info(
            "Successfully uploaded to data lake, old: %s, new: %s", myblob.name, new_file_name
        )

    except ValueError as err:
        logging.info("Error converting %s to python dictionary: %s", myblob.name, err)