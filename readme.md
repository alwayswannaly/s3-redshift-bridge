# Description
Sync your s3 bucket to redshift cluster

## Build Image
```bash
docker build -t s3-redshift-bridge .
```

## Run Docker Shell
```bash
docker run -it --volume $(pwd):/app s3-redshift-bridge sh
```

## Running on ec2 instance
```bash
pip3 install -r requirements.txt
python3 src/main.py
pm2 run ecosystem.config.js
```
