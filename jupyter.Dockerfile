# Start from a stable, well-known Jupyter image
FROM jupyter/scipy-notebook:latest

# Switch to the root user to install new packages
USER root

#
# --- INSTALL JAVA AND SET JAVA_HOME ---
#
# Update the package manager and install Java Development Kit (JDK)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless && \
    apt-get clean

# Set the JAVA_HOME environment variable for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
#
# --- END OF JAVA INSTALLATION SECTION ---
#

# THE FINAL FIX: Change the version from 3.5.1 to 3.5.0 to match the cluster
RUN pip install --no-cache-dir pyspark==3.5.0 findspark

# Switch back to the default, non-root jovyan user
USER jovyan