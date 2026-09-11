# The base image 
FROM --platform=linux/amd64 public.ecr.aws/dataminded/spark-k8s-glue:v4.0.1-hadoop-3.4.2-v4

USER 0
ENV PYSPARK_PYTHON python3

ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip

WORKDIR /opt/spark/work-dir

#TODO add your project code and dependencies to the image
COPY requirements.txt ./
RUN grep -v '^-e \.$' requirements.txt > /tmp/dependencies.txt \
    && pip install --no-cache-dir -r /tmp/dependencies.txt \
    && rm /tmp/dependencies.txt

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps .
