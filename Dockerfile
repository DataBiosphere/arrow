FROM google/cloud-sdk:265.0.0-alpine

WORKDIR /usr/src/app
COPY requirements.txt ./

# For Debian-based base image
#RUN apt-get install -qqy python3 python3-pip

# For Alpine-based base image
RUN apk --no-cache add python3
RUN apk --no-cache add --virtual build-dependencies python3-dev build-base \
  && pip3 install --no-cache-dir -r requirements.txt \
  && apk del build-dependencies

# TODO: Slim down what gets bundled
COPY . .

# From experiments deploying to Cloud Run
#CMD [ "gunicorn", "-b", ":8000", "arrow.server:app", "--worker-class", "sanic.worker.GunicornWorker", "--timeout", "120", "--log-level", "warning" ]
