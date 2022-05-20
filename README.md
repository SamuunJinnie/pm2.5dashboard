# PM 2.5 Dashboard

## Manual

### Step 0.)

Clear docker volume and images **(if needed)**

Clear Docker volume _Be aware that your **every volume** will be gone_

`docker volume rm $(docker volume ls -q)`

Clear Docker images _Be aware that your **every images** will be deleted_

`docker rmi -f $(docker images -aq)`

### Step 1.)

Build postgres image with initial command in **setup.ql**

`docker-compose build --no-cache postgres`

### Step 2.)

Build other every images and run (About 15 minutes for the first time)

`docker-compose up -d`

### Step 3.)

Open DAG in http://localhost:8080/

### Step 4.)

Watch dashboard: https://app.powerbi.com/groups/me/reports/737dc4e4-a1eb-417e-9533-0fd073b385ca?ctid=271d5e7b-1350-4b96-ab84-52dbda4cf40c&pbi_source=linkShare

### Step 5.)

**_Pray_**