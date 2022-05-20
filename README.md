# PM 2.5 Dashboard

## Manual

### Step 0.)

#### Clear docker volume and images *(if needed)*

Clear Docker volume *Be aware that your **every volume** will be gone*

`docker volume rm $(docker volume ls -q)`

Clear Docker images *Be aware that your **every images** will be deleted*

`docker rmi -f $(docker images -aq)`

### Step 1.)

Build postgres image with initial command in **setup.ql**

Please remind that this will remove exist table name raw_data and predicted_data and create the new ones.

`docker-compose build --no-cache postgres`

### Step 2.)

Build other every images and run (About 15 minutes for the first time)

`docker-compose up -d`

now you should be able to access airflow UI via `http://localhost:8080`

### Step 3.)

Log in to airflow with username : `airflow` and password : `airflow`

now you should be able to see DAGs available in the system

### Step 4.)

Trigger `hourly_dag` to start scraping data and save the data to the database (table: **raw_data**)

**Cautions! Default start date of DAG is set to <ins>14-05-2022 00.00 UTC+7</ins>**

**To change this, go to `daily_dag.py` and change `start date` argument with UTC timezone.**

### Step 5.)

Trigger `daily_dag` to feed the data to a model and start training the model.

After that the data will be saved in the database (table: **predicted_data**)

Next, the data will be sent to PowerBI report.

**Cautions! To execute this step, you need atleast 6 successive days data from the previous step.**

### Step 6.)

Open the provided PowerBI report and enjoy! :joy:

[Link to PowerBI report](https://app.powerbi.com/groups/me/reports/737dc4e4-a1eb-417e-9533-0fd073b385ca?ctid=271d5e7b-1350-4b96-ab84-52dbda4cf40c&pbi_source=linkShare)

**ps. We use PowerBI report instead of dashboard since the dashboard is not responsive. :cry:**
