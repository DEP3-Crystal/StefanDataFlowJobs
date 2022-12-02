
#################### demo-01-EnablingAPIAndInstallingClient ####################
NOTES:
-- There are a number of other APIs that need to be enabled https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven


######################
=> Go to "https://console.cloud.google.com/"
=> Login and create project
=> Go to "APIs & Services" from the side navigation and click on "Dashboard"
=> Now click on "ENABLE APIS AND SERVICES" and type "Dataflow API" in search.

=> Enable dataflow Api. It will take some time and the api will be enabled.
=> Now go to "IAM & ADMIN" and click on "Service Accounts". 
=> Click on "CREATE SERVICE ACCOUNT" and file the below details and click on "Create":

		Service account name : loony-dataflow
		Service account ID : loony-dataflow
		Service account description : Dataflow jobs, allow access to other GCP services

=> Now click on "Select a role" -> "Project" -> "Owner" and click on "Continue".
=> This field is optional so do not fill anything and click on "Done".
=> From the dashboard click on the three dots in Action for the created dataflow account and click on "Create key".
=> Key type is "JSON" and click on "Create".
=> Once the key will be created it will be downloaded automatically.

=> Open Cloud Shell and expand to full screen (top-right open in new window)

=> Set the current project. Run

gcloud config set project loony-dataflow

=> Click on three dots. Click on upload file and upload the key that we just downloaded.

=> Run command "ls -l loony-data*" and we can see the file is uploaded successfully.
=> Run command "export GOOGLE_APPLICATION_CREDENTIALS=~/loony-dataflow-review-882ff66f296d.json"
=> Run command "echo $GOOGLE_APPLICATION_CREDENTIALS"

=> Run "sudo apt-get update"
=> Run "mvn -version"

#################### demo-02-CreatingAndRunningMavenProject ####################

NOTES:
--region

A Dataflow regional endpoint stores and handles metadata about your Dataflow job, and deploys and controls your Dataflow workers.

###################################################

=> BEFORE each demo please refresh your Cloud Shell editor so it does not show any errors while recording
=> ALSO make sure that the code is full-screen (there is some window which keeps popping up at the bottom which we should close before recording)

=> Run following command one by one:

mvn archetype:generate \
    -DarchetypeGroupId=org.apache.beam \
    -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
    -DarchetypeVersion=2.23.0 \
    -DgroupId=org.example \
    -DartifactId=ApacheBeamProject \
    -Dversion="0.1" \
    -Dpackage=org.apache.beam.examples \
    -DinteractiveMode=false

 	cd ApacheBeamProject

=> Click on "Open Editor" and show "pom.xml" file and add "<project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>" in properties.
=> Scroll down and show the <profiles> and <dependencies> in this pom.xml

=> Show code in "WordCount.java".
=> Create a new folder "resources" inside main folder.
=> Create two more folder "Source" and "Sink" inside resources
=> Upload datasets in "Source" folder. Right-click on the "Source" folder -> Upload files

=> Show the contents of the files

=> The data we are taking in txt file in from "https://cts.instructure.com/courses/172185/pages/word-count-examples"
=> Run default WordCount file from terminal by below command:

mvn compile exec:java \
    -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Dexec.args="--inputFile=src/main/resources/Source/SorrowsOfWerther.txt \
    --output=src/main/resources/Sink/wordCount" \

=> Show the contents of the "Sink" folder which contains word counts

(Here we are not taking default input file because it is too big and take too much time in processing).

=> Run "gcloud auth login" and go to the link in new tab.
=> Click on allow and copy the key.
=> Paste the key in terminal and run the below command to create bucket:

	gsutil mb -p loony-dataflow-review -l us-central1 gs://loony-dataflow-storage/

=> Show the bucket is created and now upload the csv file that we are going to use in the project.

=> Create folders here "Source", "Sink"

=> Upload ALL the CSV files we are going to use across all demos in one go (will save us time later)

=> IMPORTANT: Keep one tab open on the GCP bucket so you can switch to that as needed

=> Create a new java file "BrandCount.java" (refer "BrandCount_v1.java") in examples and run it using direct runner:

mvn -Pdataflow-runner compile exec:java \
	-Dexec.mainClass=org.apache.beam.examples.BrandCount \
	-Dexec.args="--project=loony-dataflow-review \
	--stagingLocation=gs://loony-dataflow-storage/staging/ \
	--runner=DataflowRunner \
	--region=us-central1"

=> Go to Cloud Storage and show the result in the "Sink" folder

=> Open up the "staging" folder and show all the contents

=> Delete result text files in Sink folder.

=> Change "BrandCount.java" (refer "BrandCount_v2.java") and run it using dataflow again:

mvn -Pdataflow-runner compile exec:java \
	-Dexec.mainClass=org.apache.beam.examples.BrandCount \
	-Dexec.args="--project=loony-dataflow-review \
	--stagingLocation=gs://loony-dataflow-storage/staging/ \
	--runner=DataflowRunner \
	--region=us-central1"

=> Show the error in the terminal window 

mvn -Pdataflow-runner compile exec:java \
	-Dexec.mainClass=org.apache.beam.examples.BrandCount \
	-Dexec.args="--project=loony-dataflow-review \
	--stagingLocation=gs://loony-dataflow-storage/staging/ \
	--inputFile=gs://loony-dataflow-storage/Source/USA_cars_datasets.csv \
	--output=gs://loony-dataflow-storage/Sink/brandCount \
	--runner=DataflowRunner" 

=> Show the error in the terminal window (the region property is required when you want to run using the DataFlow runner)

=> Now finally run the code using the command below

mvn -Pdataflow-runner compile exec:java \
	-Dexec.mainClass=org.apache.beam.examples.BrandCount \
	-Dexec.args="--project=loony-dataflow-review \
	--stagingLocation=gs://loony-dataflow-storage/staging/ \
	--inputFile=gs://loony-dataflow-storage/Source/USA_cars_datasets.csv \
	--output=gs://loony-dataflow-storage/Sink/brandCount \
	--runner=DataflowRunner \
	--region=us-central1"

=> In this demo we will quickly look at the Dataflow UI for jobs (details in the next demo)

=> Go to Dataflow -> Jobs

=> Show the running jobs

=> Click on the running brandcount job and show the execution graph 

=> On the right hand pane just scroll from the top to the bottom

=> Wait on this page till the job is complete (no more exploration)


#################### demo-03-ShowingMetricsFromUI ####################

=> BEFORE each demo please refresh your Cloud Shell editor so it does not show any errors while recording
=> ALSO make sure that the code is full-screen (there is some window which keeps popping up at the bottom which we should close before recording)

=> Create a new java file "MonthlyAverageStockPrices.java"

=> Run the code from cloud shell:

mvn -Pdataflow-runner compile exec:java \
	-Dexec.mainClass=org.apache.beam.examples.MonthlyAverageStockPrices \
	-Dexec.args="--project=loony-dataflow-review \
	--stagingLocation=gs://loony-dataflow-storage/staging/ \
	--inputFile1=gs://loony-dataflow-storage/Source/GOOG_current_year_jan_to_aug.csv \
	--inputFile2=gs://loony-dataflow-storage/Source/GOOG_last_year_dec_data.csv \
	--output=gs://loony-dataflow-storage/Sink/sideInputResult \
	--runner=DataflowRunner \
	--region=us-central1"

=> Show all the graphs and metrics using UI. (refer recording "demo-03-ShowingMetricsFromUI")

=> Some guidance for recording

=> Close the Cloud Shell at the bottom so you have a little more room

=> Start off on the main job graph page and stay on this page till the pipeline is complete (otherwise the recording is very jumpy when you move around)

=> Slowly scroll and show all the details on the right hand side pane

=> As individual steps in the pipeline complete select each step starting from the first, go to the very end of the pipeline. Then definitely click on the two merged stages of the pipeline

=> On the RHS show the number of input elements and the number of output elements for each state

=> For the merged stage there will be some additional information on the RHS called Side Input Metric, show that as well

=> Now click on Job Metrics and show the graphs there

=> Once the job is successful show result and delete the result file.

#################### demo-04-Autoscaling ####################

NOTES:

https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#streaming-engine

#######################################

=> Rerun the job again with workers:

mvn -Pdataflow-runner compile exec:java \
	-Dexec.mainClass=org.apache.beam.examples.MonthlyAverageStockPrices \
	-Dexec.args="--project=loony-dataflow-review \
	--stagingLocation=gs://loony-dataflow-storage/staging/ \
	--inputFile1=gs://loony-dataflow-storage/Source/GOOG_current_year_jan_to_aug.csv \
	--inputFile2=gs://loony-dataflow-storage/Source/GOOG_last_year_dec_data.csv \
	--output=gs://loony-dataflow-storage/Sink/sideInputResult \
	--runner=DataflowRunner \
	--region=us-central1 \
	--maxNumWorkers=50 \
	--numWorkers=10"

=> Go to Dataflow -> Jobs

=> Show on the RHS next to the jobgraph the JobInfo which includes the pipeline options

=> Stay here till you show that the current workers (on the RHS) go up to 10

=> Then click on Job Metrics and show the graph which shows you the number of workers

=> Show the result and delete the file

mvn -Pdataflow-runner compile exec:java \
	-Dexec.mainClass=org.apache.beam.examples.MonthlyAverageStockPrices \
	-Dexec.args="--project=loony-dataflow-review \
	--stagingLocation=gs://loony-dataflow-storage/staging/ \
	--inputFile1=gs://loony-dataflow-storage/Source/GOOG_current_year_jan_to_aug.csv \
	--inputFile2=gs://loony-dataflow-storage/Source/GOOG_last_year_dec_data.csv \
	--output=gs://loony-dataflow-storage/Sink/sideInputResult \
	--runner=DataflowRunner \
	--region=us-central1 \
	--enableStreamingEngine \
	--maxNumWorkers=50 \
	--workerMachineType=n1-standard-2 \
	--diskSizeGb=30 \
	--numWorkers=10"

=> Go to Dataflow -> Jobs

=> Show on the RHS next to the jobgraph the JobInfo which includes the pipeline options "enableStreamingEngine" should be true now, other parameters that we have specified should be present as well



#################### demo-05-MoniteringJobFromCLI ####################

=> Run the same pipeline as in the previous demo

mvn -Pdataflow-runner compile exec:java \
	-Dexec.mainClass=org.apache.beam.examples.MonthlyAverageStockPrices \
	-Dexec.args="--project=loony-dataflow-review \
	--stagingLocation=gs://loony-dataflow-storage/staging/ \
	--inputFile1=gs://loony-dataflow-storage/Source/GOOG_current_year_jan_to_aug.csv \
	--inputFile2=gs://loony-dataflow-storage/Source/GOOG_last_year_dec_data.csv \
	--output=gs://loony-dataflow-storage/Sink/sideInputResult \
	--runner=DataflowRunner \
	--region=us-central1 \
	--maxNumWorkers=50 \
	--numWorkers=10"

=> Open up another tab on Cloud Shell (click on the + next to the current tab)

=> To check the logs from command line interface run the below codes:

	gcloud dataflow

	gcloud dataflow jobs

	gcloud dataflow jobs list --region=us-central-1

=> Make sure the JobId is the job that is currently running

	export JOBID=2020-10-05_06_02_18-15161882644666915891

	gcloud dataflow jobs describe $JOBID --region=us-central-1

	gcloud --format=json dataflow jobs describe $JOBID --region=us-central-1

	gcloud alpha dataflow logs list $JOBID \
	--region=us-central-1

	gcloud alpha dataflow logs list $JOBID \
	--region=us-central-1 \
	--importance=detailed

	gcloud alpha dataflow metrics list $JOBID --region=us-central-1

#################### demo-06-ShowingCloudMonitoring ####################

=> Change code in "MonthlyAverageStockPrices.java".
=> Run the job and this time we will save the json format of our job:

mvn -Pdataflow-runner compile exec:java \
	-Dexec.mainClass=org.apache.beam.examples.MonthlyAverageStockPrices \
	-Dexec.args="--project=loony-dataflow-review \
	--stagingLocation=gs://loony-dataflow-storage/staging/ \
	--inputFile1=gs://loony-dataflow-storage/Source/GOOG_current_year_jan_to_aug.csv \
	--inputFile2=gs://loony-dataflow-storage/Source/GOOG_last_year_dec_data.csv \
	--output=gs://loony-dataflow-storage/Sink/sideInputResult \
	--runner=DataflowRunner \
	--region=us-central1 \
	--dataflowJobFile=gs://loony-dataflow-storage/Sink/dataflowJob"

=> Show the logs while job is running.
=> Once job is completed move to stack driver and show the metrics. (refer recording "demo-06-ShowingCloudMonitoring")

=> Some guidance for recording this (let us make this much simpler than what you have)

=> For the number of vCPUs just show the line chart and the stacked bar. DO NOT show any other kind of chart

=> DO NOT customize the time or do any of the other stuff, move straight on to monitoring elapsed time

=> Again DO NOT show any of the other graph types

=> Move straight on to element count

=> Setting the alert is fine

=> Create Alert policy. (refer recording "demo-06-ShowingCloudMonitoring")

=> Go to gcs and how the result files. 

=> Show the json format of job.


#################### demo-07-GroupByKeySinkToPubsub ####################

=> Open up PubSub on a new tab (you should already have a tab for Dataflow and Cloud Storage)
=> From the side navigation go to pubsub and create a topic "UsedCarPrices"
=> Create subscription for this topic name it as "UsedCarPricesSub"
=> Scroll and show the subscription options

=> Create a java file "ComputeAverageMakeModelPrices.java"
=> Run it from terminal and go to dataflow dashboard.

mvn -Pdataflow-runner compile exec:java \
	-Dexec.mainClass=org.apache.beam.examples.ComputeAverageMakeModelPrices \
	-Dexec.args="--project=loony-dataflow-review \
	--stagingLocation=gs://loony-dataflow-storage/staging/ \
	--inputFilesLocation=gs://loony-dataflow-storage/Source/car_*.csv \
	--topicName=UsedCarPrices \
	--runner=DataflowRunner \
	--region=us-central1"

=> Show job graph 

=> Wait till the job graph shows some progress not too much (just stay on the page)

=> Go to PubSub
=> Click on Topics and select "UsedCarPrices"
=> Now click on view messages on top and select the subscription

=> Now keep clicking on pull. Once dataflow job will be completed we can see the messages.

=> Close the dialog where we pull messages

=> Show the Unacked message count on the main Subscription page

#################### demo-08-GroupByKeySinkToBigQuery ####################

=> Create a dataset "results" in bigQuery
=> Change code in "ExtractMakeModelPrices.java"
=> Run the code:

mvn -Pdataflow-runner compile exec:java \
	-Dexec.mainClass=org.apache.beam.examples.ExtractMakeModelPrices \
	-Dexec.args="--project=loony-dataflow-review \
	--stagingLocation=gs://loony-dataflow-storage/staging/ \
	--inputFilesLocation=gs://loony-dataflow-storage/Source/car_*.csv \
	--tableName=makeModelPrices \
	--runner=DataflowRunner \
	--region=us-central1"


=> Go to BigQuery and show the result.

=> Run this command

select * from `loony-dataflow-review.results.makeModelPrices`


#################### demo-09-ErrorsAndRetries ####################

=> Add org.json dependency to the pom.xml file (see the pom.xml with this demo)

    <dependency>
        <groupId>org.json</groupId>
        <artifactId>json</artifactId>
        <version>20200518</version>
    </dependency>

=> Create a java file "JoiningCustomerDetails.java" (refer "JoiningCustomerDetails_v1.java")
=> Run it from terminal and go to dataflow dashboard.

mvn -Pdataflow-runner compile exec:java \
	-Dexec.mainClass=org.apache.beam.examples.JoiningCustomerDetails \
	-Dexec.args="--project=loony-dataflow-review \
	--stagingLocation=gs://loony-dataflow-storage/staging/ \
	--inputFile1=gs://loony-dataflow-storage/Source/mall_customers_info.csv \
	--inputFile2=gs://loony-dataflow-storage/Source/mall_customers_score.csv \
	--tableName=mallCustomers \
	--runner=DataflowRunner \
	--region=us-central1"

=> Show job graph and wait for 3-4 mins and we will see an error 
=> Show logs (job logs and worker logs)
=> Show the error in Cloud Shell where we run the job as well
=> Stop the Job 


=> Change the code "JoiningCustomerDetails.java" (refer "JoiningCustomerDetails_v1.java")
=> Run it from terminal and this time will be completed.
=> Go to big query and show the table.
=> Click on Preview to show the records


#################### demo-10-WorkingWithSlowPipeline ####################

=> Delete the mallCustomers table which has already been created

=> Change code in "JoiningCustomerDetails.java" (add delay and logs) and run the code:

mvn -Pdataflow-runner compile exec:java \
	-Dexec.mainClass=org.apache.beam.examples.JoiningCustomerDetails \
	-Dexec.args="--project=loony-dataflow-review \
	--stagingLocation=gs://loony-dataflow-storage/staging/ \
	--inputFile1=gs://loony-dataflow-storage/Source/mall_customers_info.csv \
	--inputFile2=gs://loony-dataflow-storage/Source/mall_customers_score.csv \
	--tableName=mallCustomers \
	--runner=DataflowRunner \
	--region=us-central1"

=> From the side metrics in job graph dashboard we can see it is taking too much time compare to previous one.

=> Click on the first 3 steps of the left pipeline
=> The 3rd step will show very few elements processed (compare input elements and output elements for IdIncomeKV)

=> Click on the 3rd step of the parallel pipeline (IdScroreKV) and show the number of input elements and output elements and see that the processing is very first

=> Click on the join step and show that is slow as well

#################### demo-11-TumblingWindow ####################

=> Create a new PubSub topic StockPrices

=> Create a new java file "Windowing.java" and here we will show tumbling window.
=> Run the code from terminal:

mvn -Pdataflow-runner compile exec:java \
  	-Dexec.mainClass=org.apache.beam.examples.Windowing \
  	-Dexec.args=" \
    --project=loony-dataflow-review \
    --inputTopic=projects/loony-dataflow-review/topics/StockPrices \
    --tableName=tumblingWindowResult \
    --runner=DataflowRunner \
    --windowSize=20 \
    --region=us-central1"

=> Once the job started running and workers will be assigned we will click on the topic that we created and click on publish message


=> Open up a new Cloud Shell tab and send messages as below

=> Send all the messages in one go

gcloud pubsub topics publish StockPrices \
--message="{'company' : 'GOOG'  ,  'close' : '1400.01'}" \
--attribute=time='2020-10-01T11:35:10Z'


gcloud pubsub topics publish StockPrices \
--message="{'company' : 'GOOG'  ,  'close' : '1410.35'}" \
--attribute=time='2020-10-01T11:35:11Z'

gcloud pubsub topics publish StockPrices \
--message="{'company' : 'GOOG'  ,  'close' : '1414.25'}" \
--attribute=time='2020-10-01T11:35:12Z'

gcloud pubsub topics publish StockPrices \
--message="{'company' : 'GOOG'  ,  'close' : '1405.22'}" \
--attribute=time='2020-10-01T11:35:38Z'


gcloud pubsub topics publish StockPrices \
--message="{'company' : 'MSFT'  ,  'close' : '209.32'}" \
--attribute=time='2020-10-01T11:35:10Z'

gcloud pubsub topics publish StockPrices \
--message="{'company' : 'MSFT'  ,  'close' : '212.11'}" \
--attribute=time='2020-10-01T11:35:15Z'

gcloud pubsub topics publish StockPrices \
--message="{'company' : 'MSFT'  ,  'close' : '214.19'}" \
--attribute=time='2020-10-01T11:35:55Z'


=> Now go to dataflow jobs and selecting the current running job and fromthe side metrics check the result values has been written in big bigQuery


IMPORTANT

=> Wait on this JobGraph page till you see that the write to BigQuery for the raw data 
has 7 elements written out and for the average has at least 2 elements written out

=> Wait for some time ( atleast 2 mins) and we can see in big query two tables has been created.

=> Stop the streaming job and see the stop options are now different
=> Choose the drain option and stop the job

=> Now check result. Chraw data time and based on that check windowing is correct in other table or not.
=> Delete both tables


#################### demo-12-UpdatingRunningPipeline ####################

=> Check the jobName of the previous running job and run the following command:

mvn -Pdataflow-runner compile exec:java \
	-Dexec.mainClass=org.apache.beam.examples.Windowing \
  	-Dexec.args=" \
    --project=loony-dataflow-review \
    --inputTopic=projects/loony-dataflow-review/topics/StreamingData \
    --jobName=<JOBNAME> \
    --tableName=sessionWindowResult60 \
    --windowSize=60 \
    --runner=DataflowRunner \
    --region=us-central1 \
    --update"

=> Go to the JobGraph you will see the current status is Updating on the RHS

=> Once the job has been updated it will be grayed

-> Back to the main DataFlow jobs page and show that we have a new job running with the same job name

=> Again go to pubsub topic and publish following data one by one and wait to process:

gcloud pubsub topics publish StockPrices \
--message="{'company' : 'MSFT'  ,  'close' : '213.03'}" \
--attribute=time='2020-10-01T11:37:55Z'

=> Stop the job and check result in big query table, show the raw table


#################### demo-13-RunningPreBuiltTemplate ####################

=> Go to big query and click on create under "results" dataset
=> Enter table name "stockClosePrices" (do not change rest of the above entry)
=> Give schema as follows:

	Name: company, Type: String, Mode:Required
	Name: close, Type: Float, Mode:Nullable

=> Click on create table
=> Go to dataflow job dashboard and click on "Create Job From Templates"
=> Enter job name "stockPricesPubsubToBq-i2b2-sp57" and select region (in my case it is us-central1)
=> Select "PubSubTopic to BigQuery" and select the topic "projects/loony-dataflow-review/topics/StockPrices"

=> Enter big query table "loony-dataflow-review:results.stockClosePrices"
=> Enter temporary location "gs://loony-dataflow-storage/temp"

=> Click on Show Optional Parameters

=> Click on "RunJob"

=> Show the JobGraph in Dataflow jobs

=> Once the job start running publish following message from topic and it will be saved in big query.

gcloud pubsub topics publish StockPrices \
--message='{"company" : "GOOG"  ,  "close" : 1450.01}'

gcloud pubsub topics publish StockPrices \
--message='{"company" : "GOOG"  ,  "close" : 1453.78}'

gcloud pubsub topics publish StockPrices \
--message='{"company" : "GOOG"  ,  "close" : 1456.21}'

gcloud pubsub topics publish StockPrices \
--message='{"company" : "MSFT"}'

gcloud pubsub topics publish StockPrices \
--message='{"company" : "MSFT"  ,  "close" : 221.21}'


=> Go to big Query and show how the null value is saved

gcloud pubsub topics publish StockPrices \
--message='{"ticker" : "MSFT"  ,  "close" : 211.21}'


=> In big query one more
	table will be created "stockClosePrices_error_records" open it and show the data.

=> Delete both tables (we will recreate the tables in the next demos)

=> Stop the job

#################### demo-14-CreatingCustomTemplates ####################

=> Create a new java file "CustomPubSubToBq.java"

=> Run the code and save the template in gcs:

mvn -Pdataflow-runner compile exec:java \
  	-Dexec.mainClass=org.apache.beam.examples.CustomPubSubToBq \
  	-Dexec.args=" \
    --project=loony-dataflow-review \
    --stagingLocation=gs://loony-dataflow-storage/staging \
    --tempLocation=gs://loony-dataflow-storage/temp \
    --templateLocation=gs://loony-dataflow-storage/templates/PubSubToBigQueryTemplate \
    --runner=DataflowRunner \
    --region=us-central1"

=> Go to Google Cloud Storage and show the template is created

=> Open up the template and show the JSON

#################### demo-15-RunningCustomTemplates ####################

=> From the dataflow dashboard go to "Create Job From Templates"
=> Give job name "custompubsubtobq-i2b2-sp57" and select region 
=> Select "custom template" from the dropdown
=> Give templates location and give temporary path "gs://loony-dataflow-storage/temp"

=> Click on add parameter and add below data:

	Name: inputTopic, Value: projects/loony-dataflow-review/topics/StockPrices

=> Do not run the job yet

=> Get rid of this input parameter

=> Click on the message which says cannot find the metadata for the template

=> Cancel out of this create a job from custom template operation

=> Go to the "gs://loony-dataflow-storage/templates/" folder on Cloud Storage

=> Upload the PubSubToBigQueryTemplate_metadata file

=> Recreate the job from custom template (you need to start afresh)

=> Fill in all the details as before

=> Thanks to the presence of the template file you will see an input box asking for the inputTopic

=> Fill in "loony-dataflow-review:results.stockClosePrices"

=> And click on run job.

=> Open up Cloud Shell and publish these messages

gcloud pubsub topics publish StockPrices \
--message='{"company" : "GOOG"  ,  "close" : 1450.01}'

gcloud pubsub topics publish StockPrices \
--message='{"company" : "GOOG"  ,  "close" : 1453.78}'

gcloud pubsub topics publish StockPrices \
--message='{"company" : "MSFT"  ,  "close" : 221.21}'

=> Show the table output in BigQuery 

=> Stop the job.

#################### demo-16-RunningDataflowPipelineUsingSQLQueries ####################

=> Create a new pub sub topic "Transactions"
=> Go to big query dashboard and click on "Query Settings"
=> Select "Cloud Dataflow Engine" and enable the api. Click save.
=> Create a dataset "sales_dataset" and then click on create table "sales_regions"
=> Select create table from upload and upload the csv file "us_transactions.csv"
=> Check the box Auto detect schema and input parameters
=> Click on create.
=> Click on Add Data in resources
=> Select Add cloud dataflow source and select cloud pub/sub topics
=> Click on Edit schema and assign schema as below:

	[
  {
      "description": "Pub/Sub event timestamp",
      "name": "event_timestamp",
      "mode": "REQUIRED",
      "type": "TIMESTAMP"
  },
  {
      "description": "Transaction time string",
      "name": "tr_time_str",
      "type": "STRING"
  },
  {
      "description": "First name",
      "name": "first_name",
      "type": "STRING"
  },
  {
      "description": "Last name",
      "name": "last_name",
      "type": "STRING"
  },
  {
      "description": "City",
      "name": "city",
      "type": "STRING"
  },
  {
      "description": "State",
      "name": "state",
      "type": "STRING"
  },
  {
      "description": "Product",
      "name": "product",
      "type": "STRING"
  },
  {
      "description": "Amount of transaction",
      "name": "amount",
      "type": "FLOAT64"
  }
]

=> In the query editor write below query:

	SELECT tr.*, sr.sales_region
	FROM pubsub.topic.`loony-dataflow-review`.Transactions as tr
  		INNER JOIN bigquery.table.`loony-dataflow-review`.sales_dataset.sales_regions AS sr
  		ON tr.state = sr.state_code

=> Click on Create Dataflow Job 
=> Select BigQuery from primary output and select the dataset and give table name
=> Click on create and the job will be queued and it iwll start running after few seconds
=> Once the job started go to pubsub topic and publish following messages one by one:

	{"event_timestamp": "2020-09-29 11:49:57", "first_name": "Lilliana", "last_name": "Tonda", 
	"city": "Greenville", "state": "IL", "product": "Product 2", "amount": 592.11}

	{"event_timestamp": "2020-09-29 11:49:59", "first_name": "Angelique", "last_name": "Von", 
	"city": "Greenville", "state": "IL", "product": "Product 2", "amount": 321.22}

	{"event_timestamp": "2020-09-29 11:50:00", "first_name": "Deidre", "last_name": "Smith", 
	"city": "Greenville", "state": "IL", "product": "Product 2", "amount": 801.22}

=> Go to big query table and check the output


























