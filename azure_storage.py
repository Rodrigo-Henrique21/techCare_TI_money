
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import os
import logging
import io
import pandas as pd
from datetime import datetime

class AzureStorageManager:
    def __init__(self):
        account_url = os.environ.get("AzureWebJobsStorage_blobServiceUri")
        container_name = "stgdataprojecttimeseries"
        if not account_url:
            raise ValueError("AzureWebJobsStorage_blobServiceUri não encontrada nas variáveis de ambiente")
        credential = DefaultAzureCredential()
        self.blob_service_client = BlobServiceClient(account_url, credential=credential)
        self.container_client = self.blob_service_client.get_container_client(container_name)
        
    def save_dataframe(self, df, layer: str, name: str, format: str = 'parquet') -> None:
        """
        Salva um DataFrame diretamente no Azure Blob Storage
        """
        try:
            blob_path = f"{layer}/{name}.{format}"
            blob_client = self.container_client.get_blob_client(blob_path)
            
            if format == 'parquet':
                buffer = df.to_parquet()
            elif format == 'csv':
                buffer = df.to_csv(index=True).encode('utf-8')
            else:
                raise ValueError(f"Formato não suportado: {format}")
            
            blob_client.upload_blob(buffer, overwrite=True)
            logging.info(f"DataFrame salvo com sucesso em {blob_path}")
            
        except Exception as e:
            logging.error(f"Erro ao salvar DataFrame em {layer}/{name}: {str(e)}")
            raise
            
    def read_dataframe(self, layer: str, name: str, format: str = 'parquet') -> pd.DataFrame:
        """
        Lê um DataFrame diretamente do Azure Blob Storage
        """
        try:
            blob_path = f"{layer}/{name}.{format}"
            blob_client = self.container_client.get_blob_client(blob_path)
            
            stream = blob_client.download_blob()
            if format == 'parquet':
                return pd.read_parquet(io.BytesIO(stream.readall()))
            elif format == 'csv':
                return pd.read_csv(io.StringIO(stream.content_as_text()))
            else:
                raise ValueError(f"Formato não suportado: {format}")
        except Exception as e:
            logging.error(f"Erro ao ler DataFrame de {layer}/{name}: {str(e)}")
            raise

    def upload_file(self, file_path: str, destination_folder: str) -> None:
        """
        Uploads a single file to Azure Blob Storage
        """
        try:
            file_name = os.path.basename(file_path)
            blob_path = f"{destination_folder}/{file_name}"
            blob_client = self.container_client.get_blob_client(blob_path)
            
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            logging.info(f"Uploaded {file_name} to {blob_path}")
        
        except Exception as e:
            logging.error(f"Error uploading {file_path}: {str(e)}")
            raise

    def download_file(self, blob_path: str, local_path: str) -> None:
        """
        Downloads a single file from Azure Blob Storage
        """
        try:
            blob_client = self.container_client.get_blob_client(blob_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            with open(local_path, "wb") as file:
                data = blob_client.download_blob()
                file.write(data.readall())
            logging.info(f"Downloaded {blob_path} to {local_path}")
        
        except Exception as e:
            logging.error(f"Error downloading {blob_path}: {str(e)}")
            raise

    def list_blobs(self, prefix: str = None) -> list:
        """
        Lists all blobs in the container with optional prefix
        """
        try:
            return [blob.name for blob in self.container_client.list_blobs(name_starts_with=prefix)]
        except Exception as e:
            logging.error(f"Error listing blobs: {str(e)}")
            raise
