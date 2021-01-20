FROM python:3.7

# Set the working directory to /app
WORKDIR /liv2mong

# Copy the current directory contents into the container at /app
ADD . /liv2mong


# Pymongo installation:
RUN apt-get update
#RUN apt-get update && apt-get install -y --no-install-recommends apt-utils
RUN apt-get install libsnappy-dev -y
#RUN python -m pip install pymongo
#RUN python -m pip install pymongo[snappy,gssapi,srv,tls,zstd]

# Install the dependecies
#RUN pip install -r requirements.txt

RUN pip install -r requirements.txt

# run the command to start automation
#CMD python main.py -C 133142
CMD python main.py -A 1200
