# Topics for Big Data and IOT - Spark Streaming

## Installation
Download and Install Spark 2.4.5 with Hadoop 2.7 following instructions from [here](https://spark.apache.org/downloads.html) <br>
Python3 is required. Also install numpy using `python3 -m pip install numpy` <br>

## Running the code
## Data preparation and model training
The original data for this is from [kaggle](https://www.kaggle.com/boltzmannbrain/nab/data). Download the original dataset from there to get authentic data.<br>
The data used here is manually altered a little for ease of use <br>

### Generation of data
If in possesion of the dataset, training data for the model can be generated using the script `create_dataset.py`. It collates data from multiple CPU, Disk and Network statistic files and creates a single file. <br>
This file is used as input to train the model. A sample generated dataset is present in `data/data.tgz` (filename: trainingdata.csv) <br>
Sample run of create dataset: <br>
`python3 create_dataset.py data/realAWSCloudWatch data/trainingdata.csv` <br>

### Training KMeans model
Extract the sample training data, or create your own dataset, and run <br>
`python3 train_model.py <data file> [num clusters]` <br>
Number of clusters is set to 10 here by default. Note the output of the program, and set the label of the most populous cluster in the `EXPECTED_LABEL` field in `monitor_usage.py` <br>
The model is saved into the `kmeans.trained` folder <br>

### Running the code
The code needs to be run in two parts - the Spark Streaming Engine, and the file streamer. <br>
Start spark using `python3 monitor_usage.py <streaming directory> [window size]`. Window Size is optional, and set to 3 seconds by default <br>
The streaming directory is where Spark reads files as input to the stream processor. This has to match what is provided in the streamer program <br>
Run the streamer using `python3 multistreamer.py <streaming_directory> <files>`. <br>
The files for streaming can be found in the `data.tgz` archive in the `data` folder. Sample run is as follows: <br>
```
python3 monitor_usage.py streamdata
```

```
python3 multistreamer.py streamdata data/*_anomaly.csv
```
