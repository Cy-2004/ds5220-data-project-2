# DS5220 Data Project 2

Create, schedule, and run a containerized data pipeline in Kubernetes.

## Overview

In this project you will design, containerize, schedule, and operate a real data pipeline running inside a Kubernetes cluster on AWS. The pipeline fetches live electricity generation data from a public API once per hour, persists each reading in a MongoDB database, regenerates a visualization of the accumulating time series, and publishes both the raw data and the plot to a public S3 website.

Once set up, your job should run for at least 72 hours, collecting 72 data points.

### Learning Objectives

By the end of this project you will be able to wrangle all the elements of a working container-driven data pipeline:

1. **Provision cloud infrastructure** — launch and configure an EC2 instance, attach an Elastic IP and IAM role, and enable S3 static website hosting.
2. **Deploy and operate Kubernetes** — install K3S, inspect cluster state with `kubectl`, and understand namespaces, pods, deployments, and jobs.
3. **Containerize a Python application** — write a `Dockerfile`, build a multi-stage or single-stage image, and push it to a public container registry (GHCR).
4. **Schedule work with CronJobs** — define a Kubernetes `CronJob` manifest, control its schedule, and retrieve logs from completed job pods.
5. **Manage secrets securely** — store API keys as Kubernetes Secrets and inject them as environment variables so sensitive values never appear in code or YAML files.
6. **Persist data with a StatefulSet and Persistent Volumes** — deploy MongoDB inside the cluster, understand why StatefulSets differ from Deployments, and query stored documents with `mongosh`.
7. **Consume a REST API programmatically** — authenticate with an API key, parse JSON responses, and handle incremental data collection across repeated runs.
8. **Generate and publish data & visualizations** — produce a rolling time-series plot with `seaborn`, overwrite it on each pipeline run, and serve it via S3 website hosting.

## Setup

1. **S3 Bucket** - Create a new bucket for this project, enable it as a website, and make all files within it publicly readable. Do this by following Steps 1 and 2 in [this documentation from AWS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WebsiteAccessPermissionsReqd.html). Bucket website settings will give you a unique http:// address to your bucket.
2. **EC2 Instance** - Create a `t3.large` Ubuntu 24.04LTS instance with a 30GB boot volume (not a secondary EBS volume). Attach an Elastic IP so that your host address remains consistent. Attach a Security Group that allows full access to ports 22, 80, 8000, and 8080. Give the instance an IAM Role with full access to GET and PUT objects in the bucket created above.
3. **Install K3S** - Either by hand or via bootstrapping, install a lightweight, simplified version of Kubernetes known as K3S. This single command-line installs and runs that software:
    ```
    curl -sfL https://get.k3s.io | sh -s - --write-kubeconfig-mode 644
    ```
    Note that if you run this command via bootstrapping, your `root` user will have access to the cluster, but it can also be interactively run as the `ubuntu` user, who would then have access to the cluster. This will be apparent because the "controlling" user will have a `~/.kube/config` directory and file.
4. **Check the Status of Kubernetes** - Run the following commands to learn about your new single-node Kubernetes deployment:
    - `kubectl cluster-info`
    - `kubectl get namespaces`
    - `kubectl get deployments -A` - across all namespaces
    - `kubectl get pods` - for the default namespace
    - `kubectl get pods -A` - for all namespaces
5. **Test a Simple Scheduled Job** - Use code from the file `simple-job.yaml` in this directory and write it to a file in your EC2 instance. Note this job executes a simple command each time it runs, a "Hello" with a datetime stamp.

    Submit the CronJob with this command in the instance:
    ```
    kubectl apply -f simple-job.yaml
    ```
    Then wait a few minutes to be sure a job scheduled every 5 minutes should have run. (Hint: use the `date` command to see the server's time). You can see a list of either running or recently run `Job` pods in the default namespace with:
    ```
    ubuntu@1.2.3.4:~$ kubectl get pods
    NAME                           READY   STATUS      RESTARTS   AGE
    hello-cronjob-29582745-qdzc9   0/1     Completed   0          5m37s
    hello-cronjob-29582750-f2l9t   0/1     Completed   0          37s
    ```
    To see the output of a completed job, just copy the pod name and use the `logs` sub-command:
    ```
    ubuntu@1.2.3.4:~$ kubectl logs hello-cronjob-29582750-f2l9t
    Hello from CronJob - Tue Mar 31 13:50:01 UTC 2026 
    ```
    If you get these results, your K3S cluster and your job are running perfectly! You can now delete the test job with:
    ```
    kubectl delete -f simple-job.yaml
    ```
    
> **WHOA THERE** - Why are you making us create our own Kubernetes cluster? Why note use the one we used in class, or our laptops, or the one UVA runs?
>
> I will admit that it is fundamentally wasteful to spin up a dedicated EC2 instance for a single K8S scheduled job. But on the other hand your laptop doesn't stay up and running 24/7 or you take it to other classes. I also wanted you to have the full experience of seeing K8S from the admin side, using basic `kubectl` commands, and having a project that makes use of pods, jobs, secrets, and more. 


## Sample Data Application

The pipeline below will collect **hourly US electricity generation data by fuel type** from the [**EIA Open Data API**](https://www.eia.gov/opendata/). Each run fetches the most recent hourly snapshot for a balancing authority of your choice (e.g. PJM, MISO, ERCOT), stores it in MongoDB, regenerates a rolling plot of the full time series, and pushes both the raw data and the plot image to your S3 website bucket. After 72 runs (over 72 hours) you will have a complete three-day picture of how your region's grid mixed solar, wind, gas, coal, nuclear, and hydro power generation.

### 1. Register for an EIA API Key

1. Go to [https://www.eia.gov/opendata/](https://www.eia.gov/opendata/) and click **Register**.
2. Fill in your name, email address, and organization (your university is fine).
3. An API key is emailed to you within a few minutes. It looks like a 32-character alphanumeric string.
4. Test it in a browser — replace `YOUR_KEY` below and confirm you get JSON back:
    ```
    https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/?api_key=YOUR_KEY&frequency=hourly&data[0]=value&facets[respondent][]=PJM&sort[0][column]=period&sort[0][direction]=desc&length=1
    ```

**Balancing authority options** — pick one and use it consistently throughout your project:

| Code | Region |
|------|--------|
| PJM  | Mid-Atlantic + Midwest |
| MISO | Midcontinent |
| ERCO | Texas (ERCOT) |
| NYIS | New York |
| CISO | California |
| ISNE | New England |
| SWPP | Southwest Power Pool |

### 2. Store the API Key as a Kubernetes Secret

Never put API keys directly in a YAML file or a Docker image. Kubernetes Secrets store sensitive values separately and inject them as environment variables at runtime.

Create the secret from the command line on your EC2 instance:
```
kubectl create secret generic eia-secret \
  --from-literal=EIA_API_KEY=your_key_here
```

Verify the secret was created (Kubernetes will not show the value, only that the key exists):
```
kubectl get secret eia-secret
kubectl describe secret eia-secret
```

To inspect the stored value if needed:
```
kubectl get secret eia-secret -o jsonpath='{.data.EIA_API_KEY}' | base64 --decode
```

### 3. How the Secret Flows into the CronJob

In `pipeline-job.yaml` the CronJob spec references the secret by name. Kubernetes injects it as an environment variable into the container at startup — your Python script reads it with `os.environ["EIA_API_KEY"]` and the key value never appears in any file on disk:

```yaml
containers:
  - name: pipeline
    image: ghcr.io/USERNAME/ds5220-pipeline:latest
    env:
      - name: EIA_API_KEY
        valueFrom:
          secretKeyRef:
            name: eia-secret      # the Secret object created above
            key: EIA_API_KEY      # the key within that Secret
      - name: EIA_RESPONDENT
        value: "PJM"              # plain env vars go here (not secret)
      - name: S3_BUCKET
        value: "your-bucket-name"
```

Note that `S3_BUCKET` and `EIA_RESPONDENT` are not secrets — they go directly in the spec as plain `value:` fields. Only the API key uses `secretKeyRef`. Your EC2 instance's IAM role already grants S3 access, so no AWS credentials need to appear anywhere.

For local testing you can use [**MongoDB Atlas**](), a free MongoDB provider, and set a local `ENV` variable to provide the `MONGO_URI` given to you by Atlas, along with the other `ENV` variables required by that script. Runs of `python fetch.py` should generate output files, ship them to S3, and insert data into Mongo.



### 4. Build and Push Your Pipeline Container

Your pipeline code lives in a `pipeline/` subdirectory. After writing `fetch.py` and a `Dockerfile`, build and push the image to GHCR so Kubernetes can pull it, or set up GitHub Actions to build and push on your behalf.

This presents an obstacle: If your local computer has an `amd64` chip (not a newer Mac), you can build and push your pipeline container by hand. But if your chip is `arm64` then you need to figure out a method for building an `amd64` based container image.

> **HINT**: [Lab 6](https://github.com/nmagee/fastapi-demo/blob/main/LAB.md) walks you through these steps.

```
# from an amd64 machine
cd pipeline/
docker login ghcr.io   # asks for your github username and a PAT token
docker build -t ghcr.io/USERNAME/ds5220-pipeline:latest .
docker push username/ds5220-pipeline:latest
```

Once pushed, find the container image listed under "Packages" in your GitHub profile page. Under "Package Settings" change its visibility settings to "Public". You will only need to set this once.

The container image must be publicly available for your Kubernetes cluster to pull it. (Note: there are ways of using another K8S Secret so that K8S can pull private images, but we'll save that for another course)

### RECAP - Grab a cup of coffee

You have now created and configured:


1. A website-enabled S3 bucket.
2. An EC2 instance running Kubernetes.
3. A MongoDB database to store data with a web UI to browse the database.
4. A data ingestion job with an API key.
5. A public container image containing that job.

You are now ready to publish your job on a schedule.

### 5. Deploy the Pipeline CronJob

Once your image is pushed and the EIA secret exists in the cluster:
```
kubectl apply -f pipeline-job.yaml
```

Monitor runs:
```
kubectl get cronjobs
kubectl get pods
kubectl logs <pod-name>
```

Confirm that your hourly jobs are running (the `pods` output will confirm a `Completed` status, and the `logs` output is most helpful to check for errors).

## Your Data Application

Next, find another data source that changes at least hourly **and write a new, completely different data application**. It should take roughly the same general form as the sample above - a containerized task that runs on a schedule (at least 1x per hour for 72 hours), and renders data and a single, evolving, plot back to S3. It is not required that your new data application use an API key. However, if you want to connect to an external database such as MongoDB Atlas, simply pass connection information into K8S as a secret and consume that secret within your pod as an environment variable.

Some suggestions of other data sources:

- **Open-Meteo Weather API** — fetch hourly temperature, wind speed, precipitation, or cloud cover for any lat/lon without an API key. [https://open-meteo.com/en/docs](https://open-meteo.com/en/docs)
- **USGS Water Services** — stream gauge readings updated every 15 minutes for thousands of rivers and streams across the US. [https://waterservices.usgs.gov/rest/IV-Service.html](https://waterservices.usgs.gov/rest/IV-Service.html)
- **OpenAQ Air Quality** — real-time PM2.5, ozone, NO₂, and other pollutant readings from monitoring stations worldwide, updated sub-hourly. [https://docs.openaq.org/](https://docs.openaq.org/)
- **OpenSky Network Flight Data** — live positions, altitudes, and velocities for all ADS-B-tracked aircraft currently in the air. [https://openskynetwork.github.io/opensky-api/](https://openskynetwork.github.io/opensky-api/)
- **NOAA Tides and Currents** — observed and predicted water levels at tide stations around the US coast, updated every 6 minutes. [https://api.tidesandcurrents.noaa.gov/api/prod/](https://api.tidesandcurrents.noaa.gov/api/prod/)
- **CoinGecko Crypto Prices** — free, no-key-required endpoint returning current prices, market cap, and 24-hour volume for any cryptocurrency. [https://www.coingecko.com/en/api/documentation](https://www.coingecko.com/en/api/documentation)
- **Transport for London (TfL) Unified API** — live crowding levels, arrival predictions, and disruptions across the London Underground and bus network. [https://api.tfl.gov.uk/](https://api.tfl.gov.uk/)

Just like the sample data application above, your own data application should store output data and a plot in your S3 bucket website.

## Deliverables

### All Students

Submit the following in the Canvas assignment:

1. **EIA Energy Plot URL** — the public `http://` URL to your `plot.png` file served from your S3 website bucket (e.g., `http://your-bucket-name.s3-website-us-east-1.amazonaws.com/plot.png`). The plot must show at least 72 hours of hourly readings across multiple fuel types. Paste the URL so it can be opened directly in a browser — if the image does not load, the deliverable will not be graded.

2. **Your Data Application Plot URL** - the public `http://` URL to your `plot2.png` file served from your S3 website bucket (e.g., `http://your-bucket-name.s3-website-us-east-1.amazonaws.com/plot2.png`). The plot must show at least 72 hours of hourly readings across multiple data points. Paste the URL so it can be opened directly in a browser.

3. **Your Data Application Repo URL** - the public GitHub URL to the code for your custom data application. That repository should include the application code itself (in Python), a Dockerfile, and any requirements.txt or supporting files.

### Graduate Students

In addition to the above requirements:

1. **Submit answers to the following questions** in a markdown or PDF file uploaded to Canvas. Do not consult generative AI for these answers.

    - In the sample data application above, data persists outside of the pods through the use of S3. If this were a higher-frequency application (hundreds of times per minute) how might you persist the data in a more performant way? (One paragraph)
    - How do your CronJob pods gain permission to read/write to Amazon S3? How does this differ from running a container locally using Docker? (One paragraph)
    - How Kubernetes Secrets differ from plain environment variables and why does that distinction matter? (One paragraph)
    - What is one thing that you might do differently if you were building this pipeline for real production use? (One paragraph)
