#!/bin/bash

AWS_ECR_URI= # credentials
IMAGE_NAME= # credentials
TAG_NAME= # credentials
AWS_ACCESS_KEY_ID= # credentials
AWS_SECRET_ACCESS_KEY= # credentials
AWS_S3_BUCKET= # credentials
AWS_REGION= # credentials

echo $AWS_REGION
DOCKER_IMAGE=$IMAGE_NAME:$TAG_NAME
AWS_ECR_FULL_URI=$AWS_ECR_URI/$DOCKER_IMAGE

echo "remove image"
docker rmi -f $AWS_ECR_FULL_URI
docker rmi -f $DOCKER_IMAGE

echo "build image"
docker build -t $DOCKER_IMAGE .

echo "login aws ecr"
aws ecr get-login-password --region $AWS_REGION | \
docker login --username AWS --password-stdin $AWS_ECR_URI

echo "docker push"
docker image tag $DOCKER_IMAGE $AWS_ECR_FULL_URI
docker push $AWS_ECR_FULL_URI