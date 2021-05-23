#FROM gcr.io/aml-innov-bpid-810834/spark-operator-withgcs
# CAN not be spark-operator-withgcs
FROM gcr.io/aml-innov-bpid-810834/gcsjobapp-gke
# Downloading gcloud package
#


ENV APP_HOME /home/infra-build/myapp
WORKDIR $APP_HOME
COPY . /opt/spark/examples/src/main/python/

#
