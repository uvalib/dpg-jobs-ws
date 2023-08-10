#if [ -z "$DOCKER_HOST" ]; then
#   echo "ERROR: no DOCKER_HOST defined"
#   exit 1
#fi

~/dev/virgo4-dev/terraform-infrastructure/scripts/ecr-authenticate.ksh

# set the definitions
INSTANCE=dpg-jobs-ws
NAMESPACE=115119339709.dkr.ecr.us-east-1.amazonaws.com/uvalib
TAG=build-20230810164757

if [ $# -eq 1 ]; then
  TAG=$1
fi

IMAGE=$NAMESPACE/$INSTANCE:$TAG

echo "Using $IMAGE..."
docker run -ti -p 8080:8080 -v /Users/lf6f/dev/jp2_test:/mnt/images $IMAGE /bin/bash -l
