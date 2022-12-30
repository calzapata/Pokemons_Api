FROM python:3.9.12-slim

RUN apt-get update && \
    apt-get install -y libpq-dev gcc

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

CMD [ "python3", "pokemons.py"]