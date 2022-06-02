# Installation
```bash
git clone https://github.com/Vlad-Misiukevich/m13_sparkstreaming_python_azure.git
```
# Requirements
* Windows OS
* Python 3.8
* kubernetes-cli
* azure-cli
* terraform
# Description
1. Login to Azure:  
`az login`
![img.png](images/az_login.png)
2. Deploy infrastructure with terraform:  
`terraform init`  
`terraform plan -out terraform.plan`  
`terraform apply terraform.plan`
![img_1.png](images/terraform.png)
3. Incremental copy of hotel/weather data from Azure ADLS gen2 storage into provisioned with terraform Azure ADLS gen2 storage with a delay(30 seconds), one day per cycle:  
`.\src\main\python\job.py`  
![img.png](images/img.png)  
![img_1.png](images/img_1.png)
4. Create Spark Structured Streaming application in Databricks:  
![img_2.png](images/img_2.png)
5. Calculating number of distinct hotels in the city for each city each day:  
![img_3.png](images/img_3.png)
6. The same calculating in 10 minutes (the data is changing):
![img_5.png](images/img_5.png)
7. Data is over and stream is waiting for a new data:  
![img_7.png](images/img_7.png)
8. Calculating average/max/min temperature in the city in the city for each city each day:
![img_4.png](images/img_4.png)  
9. The same calculating in 10 minutes (the data is changing):  
![img_6.png](images/img_6.png)
10. Data is over and stream is waiting for a new data: 
![img_8.png](images/img_8.png)
11. Preparing data for visualisation:  
![img_14.png](images/img_14.png)
12. Data visualization for 10 biggest cities (in example Paris, London, Barcelona, Milan, Amsterdam are shown):  
* Paris
![img_9.png](images/img_9.png)  
* London
![img_10.png](images/img_10.png)
* Barcelona
![img_11.png](images/img_11.png)
* Milan
![img_12.png](images/img_12.png)
* Amsterdam
![img_13.png](images/img_13.png)
13. Deploying Databricks Notebook on cluster:  
`terraform plan -out terraform.plan`  
`terraform apply terraform.plan` 
![img_15.png](images/img_15.png)
![img_16.png](images/img_16.png)

