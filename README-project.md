1. Data Source Summary

This project uses data from the U.S. Geological Survey Water Services API. The API provides real-time measurements of river water levels across the United States. The data used in this project is updated around every 15 minutes and does not require an API key.

2. Scheduled Process

The application runs as a Kubernetes CronJob that executes every 15 minutes. Each run performs the following steps:

- Fetch the latest water level measurement from the USGS API
- Store the data in a DynamoDB table
- Retrieve historical data from DynamoDB
- Generate a time-series plot of water level over time
- Upload the plot (plot.png) to the S3 bucket
- Generate and upload a CSV file (data.csv) containing all collected data to the S3 bucket

This process repeats automatically, allowing the dataset and visualization to continuously update.

3. Output Data and Plot

plot.png: A line chart showing water level over time. This plot updates with each run and shows all the data collected.

data.csv: A dataset containing all recorded observations, including timestamp and water level. This file updates with each run and grows over time. 



1. Which data source you chose and why

I used the U.S. Geological Survey water services API. I chose it because it updates very frequently (every ~15 minutes), does not require an API key, and provides real-world environmental data. It was also reliable compared to other APIs I tried that had rate limits or connection issues.

2. What you observe in the data

The water level stays mostly stable with small changes. There are slight increases and decreases over time, but no extreme spikes. This makes sense because river levels usually change gradually unless there is heavy rainfall or flooding. 

3. Kubernetes Secrets vs environment variables

Kubernetes Secrets are used to store sensitive information like API keys, while plain environment variables are just normal values stored in the container. Kubernetes Secrets are secure and hidden, while environment variables are visible in configuration files. This matters because putting sensitive data directly in YAML files or Docker images is unsafe. Kubernetes Secrets keep that information protected.

4. How pods access AWS without credentials

The pods use an IAM Role attached to the EC2 instance. This role gives permission to access services like S3 and DynamoDB. 

5. One thing I would do differently in production

I would add better error handling and retry logic for API requests. Right now, if the API times out or fails, the job just skips that run. In a real system, I would retry a few times before giving up to make the pipeline more reliable.