# jupyter.Dockerfile
FROM jupyter/pyspark-notebook:spark-3.5.0

# Install findspark
RUN pip --no-cache-dir install findspark jupyterlab