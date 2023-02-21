# DockerFile

From python:3.8.16

# set work directory
WORKDIR /code

# set env variable
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

#install dependencies
COPY requirements.txt /code
RUN pip install -r requirements.txt

COPY load_data.py /code
# copy project
COPY . /code
