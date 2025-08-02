import os
from azure.storage.blob import ContainerClient


BASE_URL = "https://stgdataprojecttimeseries.blob.core.windows.net/techcare-ti-money"


def _get_container_client() -> ContainerClient:
    sas_token = os.environ.get("AZURE_SAS_TOKEN")
    if not sas_token:
        raise EnvironmentError("Variável de ambiente AZURE_SAS_TOKEN não definida")
    return ContainerClient.from_container_url(f"{BASE_URL}?{sas_token}")


def upload_directory(local_path: str, prefix: str) -> None:
    """Envia todos os arquivos de ``local_path`` para o container sob ``prefix``."""
    container = _get_container_client()
    for root, _, files in os.walk(local_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            blob_path = os.path.join(prefix, os.path.relpath(file_path, local_path)).replace("\\", "/")
            with open(file_path, "rb") as data:
                container.upload_blob(name=blob_path, data=data, overwrite=True)
                print(f"Enviado: {blob_path}")


def download_directory(prefix: str, local_path: str) -> None:
    """Baixa todos os blobs com ``prefix`` para ``local_path``."""
    container = _get_container_client()
    os.makedirs(local_path, exist_ok=True)
    for blob in container.list_blobs(name_starts_with=prefix):
        rel_path = blob.name[len(prefix):]
        file_path = os.path.join(local_path, rel_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(container.download_blob(blob.name).readall())
            print(f"Baixado: {blob.name}")


if __name__ == "__main__":
    base_dir = os.path.join(os.path.dirname(__file__), "datalake", "raw")
    upload_directory(base_dir, "raw/")