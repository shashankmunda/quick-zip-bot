FROM ubuntu:22.04

RUN add-apt-repository multiverse && apt-get update && apt-get install -y \
    unrar \
    rar \
    wget \
    && apt-get clean

ENV PATH="/usr/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD [ "python", "src/bot.py" ]
