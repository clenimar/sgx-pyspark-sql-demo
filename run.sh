#!/bin/bash

# NOTE: this is tailored to the cpec cluster!

set -ex

#
# configuration
#
PYSPARK_IMAGE=godatadriven/pyspark:3.1
export DRIVER_IMAGE=registry.scontain.com:5050/clenimar/test:pyspark-3.1.1-alpine3.11-scone5.6.0plus-driver
# use the same image both for exec and driver
export EXECUTOR_IMAGE=registry.scontain.com:5050/clenimar/test:pyspark-3.1.1-alpine3.11-scone5.6.0plus-driver

#export SCONE_CAS_ADDR="5-5-0.scone-cas.cf"
#export SCONE_CAS_MRENCLAVE="783c797f68cc700afdc3067c02a4fcf77ec01f4f880debb5c0ce36bd866e794b"
export SCONE_CAS_ADDR="5-6-0.scone-cas.cf"
export SCONE_CAS_MRENCLAVE="0902eec722b3de0d11b56aab3671cc8540e62bd2333427780c8a9cd855e4f298"

rm -rf build
mkdir -p build/policies build/kubernetes build/cas build/images/driver/fspf/encrypted-files build/images/executor

#cp ~/subtree-scone/built/cross-compiler/x86_64-linux-musl/lib/misc/libspawnhelper.so .
#cp ~/subtree-scone/built/cross-compiler/x86_64-linux-musl/lib/libc.scone-x86_64.so.1 .
#cp ~/subtree-scone/built/cross-compiler/x86_64-linux-musl/bin/ld-scone-x86_64.so.1 .

docker build . -t $DRIVER_IMAGE-base -f Dockerfile

docker run --rm -t --entrypoint bash -v $PWD:/fspf -v $PWD/build/images/driver/fspf:/out $DRIVER_IMAGE-base /fspf/main_fspf.sh
docker run --rm -t --entrypoint bash -v $PWD/input:/input -v $PWD:/script -v $PWD/build/images/driver/fspf:/out -v $PWD/build/images/driver/fspf:/fspf $DRIVER_IMAGE-base /script/fspf.sh

mv build/images/driver/fspf/volume.fspf build/images/driver/fspf/encrypted-files

echo "FROM $DRIVER_IMAGE-base" > build/images/driver/Dockerfile
echo "ADD fspf/fspf.pb /" >> build/images/driver/Dockerfile
echo "ADD fspf/encrypted-files /fspf/encrypted-files" >> build/images/driver/Dockerfile

pushd build/images/driver
docker build . -t $DRIVER_IMAGE
popd

export DRIVER_PYTHON_MRENCLAVE=$(docker run --rm -t --entrypoint bash -e SCONE_HASH=1 -e SCONE_HEAP=1G $DRIVER_IMAGE -c "python3")
export DRIVER_JAVA_MRENCLAVE=$(docker run --rm -t --entrypoint bash -e SCONE_HASH=1 $DRIVER_IMAGE -c "/usr/lib/jvm/java-1.8-openjdk/bin/java")

export DRIVER_MAIN_FSPF_KEY=$(cat build/images/driver/fspf/main_fspf_keytag.txt | awk '{print $11}')
export DRIVER_MAIN_FSPF_TAG=$(cat build/images/driver/fspf/main_fspf_keytag.txt | awk '{print $9}')

export DRIVER_VOLUME_FSPF_KEY=$(cat build/images/driver/fspf/volume_keytag.txt | awk '{print $11}')
export DRIVER_VOLUME_FSPF_TAG=$(cat build/images/driver/fspf/volume_keytag.txt | awk '{print $9}')

export EXECUTOR_JAVA_MRENCLAVE=$DRIVER_JAVA_MRENCLAVE

#export EXECUTOR_JAVA_MRENCLAVE=$(docker run --rm -t --entrypoint bash -e SCONE_HASH=1 $EXECUTOR_IMAGE -c "/usr/lib/jvm/java-1.8-openjdk/bin/java")
#EXECUTOR_FSPF_KEYTAG=$(docker run --rm -t --entrypoint bash $EXECUTOR_IMAGE -c "cat /fspf/keytag.txt")
#EXECUTOR_FSPF_KEY=$(cat DRIVER_FSPF_KEYTAG | awk '{print $11}')
#EXECUTOR_FSPF_TAG=$(cat DRIVER_FSPF_KEYTAG | awk '{print $9}')

export CAS_NAMESPACE="pyspark-azure-$RANDOM$RANDOM"
export PYSPARK_SESSION_NAME="pyspark"

envsubst '$CAS_NAMESPACE' < policies/namespace.yaml.template > build/policies/namespace.yaml
envsubst '$SCONE_CAS_ADDR $CAS_NAMESPACE $PYSPARK_SESSION_NAME $DRIVER_MAIN_FSPF_KEY $DRIVER_MAIN_FSPF_TAG $DRIVER_VOLUME_FSPF_KEY $DRIVER_VOLUME_FSPF_TAG $DRIVER_JAVA_MRENCLAVE $DRIVER_PYTHON_MRENCLAVE $EXECUTOR_JAVA_MRENCLAVE' < policies/pyspark.hw.yaml.template > build/policies/pyspark.yaml

#FIXME: why does bash add \r to env vars?
sed -i 's/\r//g' build/policies/pyspark.yaml 

docker run --rm -t -e SCONE_CLI_CONFIG=/cas/config.json -v $PWD/build/cas:/cas --entrypoint bash $DRIVER_IMAGE -c "scone cas attest $SCONE_CAS_ADDR $SCONE_CAS_MRENCLAVE -GCS --only_for_testing-debug --only_for_testing-ignore-signer"
docker run --rm -t -e SCONE_CLI_CONFIG=/cas/config.json -v $PWD/build/cas:/cas --entrypoint bash -v $PWD/build/policies:/policies $DRIVER_IMAGE -c "scone session create /policies/namespace.yaml"
docker run --rm -t -e SCONE_CLI_CONFIG=/cas/config.json -v $PWD/build/cas:/cas --entrypoint bash -v $PWD/build/policies:/policies $DRIVER_IMAGE -c "scone session create /policies/pyspark.yaml"

export SCONE_CONFIG_ID="$CAS_NAMESPACE/$PYSPARK_SESSION_NAME/nyc-taxi-yellow"

envsubst < properties.hw.template > build/properties

echo 'spark.kubernetes.container.image.pullSecrets sconeapps' >> build/properties

docker push $DRIVER_IMAGE
#docker push $EXECUTOR_IMAGE

kubectl apply -f kubernetes/rbac.yaml

cp kubernetes/hw-sgx-pod-template.yaml build/kubernetes

MASTER_ADDRESS=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')

export KUBECONFIG_DIR=~/.kube

docker run -it --rm -v $KUBECONFIG_DIR:/root/.kube -v $(pwd)/build:/build $PYSPARK_IMAGE --master k8s://$MASTER_ADDRESS --deploy-mode cluster --name spark-ml --properties-file /build/properties local:///fspf/encrypted-files/nyc-taxi-yellow-model.py
