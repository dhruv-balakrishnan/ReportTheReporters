# ReportTheReporters

A project that takes a look at what reporters publish, and builds profiles based on that. The idea is to identify which reporters publish which articles, thereby creating profiles for each that might show:
- Their general stance (Between Hard-Left, Moderate-Left, Centrist, Moderate-Right, Hard-Right, other)
- Their 'favourite' topic
- Their most used words
- etc.

### Requirements
* Python >= 3.7 and appropriate PYTHONPATH env var
* Spark 2.4.4 built for Hadoop 2.7.3 and SPARK_HOME env var
* Hadoop Windows binaries and HADOOP_HOME env var
* Package requirements can be met with `pip install -r requirements.txt`

### Running the Program
* Run review_client.py
