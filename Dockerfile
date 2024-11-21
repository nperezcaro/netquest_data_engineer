FROM python:3.12.3-bullseye

COPY . /usr/src/app
WORKDIR /usr/src/app

RUN useradd -M -d /usr/src/app python && pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

USER python

CMD ["python", "main.py"]
