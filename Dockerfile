FROM public.ecr.aws/dataminded/spark-k8s-glue:v4.0.1-hadoop-3.4.2-v4

USER 0
ENV PYSPARK_PYTHON python3
ENV PYTHONPATH=/opt/spark/work-dir/src
WORKDIR /opt/spark/work-dir

#TODO add your project code and dependencies to the image
COPY pyproject.toml uv.lock README.md ./
RUN pip install --no-cache-dir "uv==0.5.11" \
    && uv sync --frozen --no-install-project

COPY src ./src
RUN uv sync --frozen
