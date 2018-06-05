FROM python:3-alpine

RUN adduser -D ib

WORKDIR /home/ib

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY app app
COPY migrations migrations
COPY com-stouffcapital-ib.py config.py boot.sh ./
RUN chmod a+x boot.sh

ENV FLASK_APP com-stouffcapital-ib.py

RUN chown -R ib:ib ./
USER ib

EXPOSE 5000
ENTRYPOINT ["./boot.sh"]
