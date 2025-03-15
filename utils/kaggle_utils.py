import kaggle

def fetch_data():
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files('olistbr/brazilian-ecommerce',path='./data', unzip=True)