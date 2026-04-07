FROM python:3.12-slim-bookworm

ARG USERNAME=dataeng
ARG UID=1000
ARG GID=1000
ARG AIRFLOW_VERSION=3.0.6
ARG AIRFLOW_CONSTRAINTS_PYTHON=3.12

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        unzip \
        git \
        build-essential \
        procps \
        less \
        groff \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark / Spark session mode
RUN ln -sfn /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

RUN groupadd --gid ${GID} ${USERNAME} \
    && useradd --uid ${UID} --gid ${GID} --create-home --shell /bin/bash ${USERNAME}

# Install AWS CLI v2 with architecture-aware download.
RUN set -eux; \
    arch="$(uname -m)"; \
    case "$arch" in \
      x86_64) aws_arch="x86_64" ;; \
      aarch64|arm64) aws_arch="aarch64" ;; \
      *) echo "Unsupported architecture: $arch"; exit 1 ;; \
    esac; \
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${aws_arch}.zip" -o /tmp/awscliv2.zip; \
    unzip -q /tmp/awscliv2.zip -d /tmp; \
    /tmp/aws/install; \
    rm -rf /tmp/aws /tmp/awscliv2.zip

# Install AWS Session Manager plugin with architecture-aware package selection.
RUN set -eux; \
    arch="$(uname -m)"; \
    case "$arch" in \
      x86_64) ssm_pkg_url="https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_64bit/session-manager-plugin.deb" ;; \
      aarch64|arm64) ssm_pkg_url="https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_arm64/session-manager-plugin.deb" ;; \
      *) echo "Unsupported architecture: $arch"; exit 1 ;; \
    esac; \
    curl -fsSL "$ssm_pkg_url" -o /tmp/session-manager-plugin.deb; \
    apt-get update; \
    apt-get install -y --no-install-recommends /tmp/session-manager-plugin.deb; \
    rm -f /tmp/session-manager-plugin.deb; \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r /tmp/requirements.txt \
    && pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${AIRFLOW_CONSTRAINTS_PYTHON}.txt"

WORKDIR /workspaces/dbt

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

USER ${USERNAME}
