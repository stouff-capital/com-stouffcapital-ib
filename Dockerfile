FROM python:3
LABEL maintainer="gregory.chevalley+docker@gmail.com"

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY app app
COPY migrations migrations
COPY com-stouffcapital-ib.py config.py boot.sh ./
RUN chmod a+x boot.sh

ENV FLASK_APP com-stouffcapital-ib.py

#ENTRYPOINT ["python"]
#CMD ["com-stouffcapital-ib.py"]

ENTRYPOINT ["./boot.sh"]
