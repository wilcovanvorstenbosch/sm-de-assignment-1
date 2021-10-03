# This API container fetches training data and returns a prediction.

# Allow certain ports for the API:

gcloud compute firewall-rules create flask-port-1 --allow tcp:5000
gcloud compute firewall-rules create flask-port-2 --allow tcp:5001

# How to build and run:

cd api
docker build . -t api
docker run –t –p 5000:5001 api

# Create a virtual network between the two containers

docker network create mynetwork
docker network connect mynetwork #state name of training container
docker network connect mynetwork #state name of api container

