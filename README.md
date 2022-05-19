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

**_Pray_**
