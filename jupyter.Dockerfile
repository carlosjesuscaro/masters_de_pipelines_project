# jupyter.Dockerfile
FROM jupyter/pyspark-notebook:spark-3.5.0

# Install findspark
RUN pip install findspark