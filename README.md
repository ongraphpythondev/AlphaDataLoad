## Load data to MongoDB
### load data to MongoDb using pyspark streaming

### Clone project using following command
```
git clone https://github.com/ongraphpythondev/AlphaDataLoad.git
```
### Make sure java is install on your system or you can install using following command
```
sudo apt install default-jdk
```
### Create .env file
```
MONGO_URL = <Your url>
```
### Create virtual enviorment and activate using following command
```
python3 -m venv venv
source venv/bin/activate
```

### Install dependencies
```
pip install -r requirements.txt
```

### Run the code 
```
python3 load_data.py
```