
FROM python:3
MAINTAINER Greg Chevalley "gregory.chevalley+docker@gmail.com"
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt

RUN chmod +x ./boot.sh

EXPOSE 5000

#ENTRYPOINT ["python"]
#CMD ["com-stouffcapital-ib.py"]

ENTRYPOINT ["./boot.sh"]
