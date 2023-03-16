# filter_version

Description:
The script filters files that are received in the "raw" folder in Azure Blob storage and that have a specific prefix. If the file has multiple versions, selects the latest version. You must specify the name of the container in Azure Blob Storage, the path to the destination directory, and the list of file prefix.

### How to use:

* Clone the repository and go to it on the command line:
bash ```
git clone https://github.com/feyaschuk/filter_version.git
```
bash ```
cd filter_version
```
* Create and activate virtual environment:
bash ```
python3 -m venv env
```
bash ```
source env/bin/activate (MAC OC, Linux) // source venv/Scripts/activate (Windows)
```
bash ```
python3 -m pip install --upgrade pip
```
* Install dependencies from requirements.txt file:
bash ```
pip install -r requirements.txt
```
* Add your SecretCredentials in row in file "filter_version.py"
bash ```
token_credential = ClientSecretCredential("{tenant-id}", "{client-id}", "{client-secret}"
```
* Run the program:
bash ```
python filter_version.py
```
* Example of usage:
python filter_version.py 
