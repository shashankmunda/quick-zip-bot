FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    unrar \
    rar \
    wget \
    && apt-get clean

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD [ "python", "src/bot.py" ]
