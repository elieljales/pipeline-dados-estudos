import zipfile
import requests


def download_file(url, output_path):
    with open(f"{output_path}/{url.split('/')[-1]}", 'wb') as f:
        r = requests.get(url)   
        f.write(r.content)
        f.close()
        return f"{output_path}/{url.split('/')[-1]}"
    
def unzip_file(input_file, output_path):
    with zipfile.ZipFile(input_file, 'r') as zip_ref:
        zip_ref.extractall(output_path)
    print("Files extracted successfully.")

if __name__ == "__main__":
    # download_file(
    #     url = "https://dadosabertos.rfb.gov.br/CNPJ/Empresas0.zip",
    #     output_path="./tmp/staged"
    # )

    unzip_file(
        "./tmp/staged/Empresas0.zip",
        "./tmp/raw"
    )
