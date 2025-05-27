FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY client.py ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./client.py" ]