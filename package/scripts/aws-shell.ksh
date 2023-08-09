#if [ -z "$DOCKER_HOST" ]; then
#   echo "ERROR: no DOCKER_HOST defined"
#   exit 1
#fi

~/Sandboxes/terraform-infrastructure/scripts/ecr-authenticate.ksh

# set the definitions
INSTANCE=dpg-jobs-ws
NAMESPACE=115119339709.dkr.ecr.us-east-1.amazonaws.com/uvalib
TAG=latest

if [ $# -eq 1 ]; then
  TAG=$1
fi

IMAGE=$NAMESPACE/$INSTANCE:$TAG

echo "Using $IMAGE..."
docker run -ti -p 8080:8080 $IMAGE /bin/bash -l
