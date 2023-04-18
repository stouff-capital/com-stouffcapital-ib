# ib webservice

## set .env file

- `SECRET_KEY`
- `DATABASE_URL="mysql+mysqlconnector://ibuser:<password>@localhost/ibdb"`
- `BASIC_AUTH_USERNAME=<authUser>`
- `BASIC_AUTH_PASSWORD=<authPassword>`

## mysql db

`docker run --name ib-mysql -e MYSQL_RANDOM_ROOT_PASSWORD=yes -e MYSQL_DATABASE=ibdb -e MYSQL_USER=ibuser -e MYSQL_PASSWORD=<mysqlPassword> -p 3306:3306 -d mysql:5.6`

## phpmyadmin

`docker run --name ib-myadmin -d --link ib-mysql:db -p 8080:80 phpmyadmin/phpmyadmin`

## container backend

`docker build -t gchevalley/com-stouffcapital-ib .`

`docker run --name com-stouffcapital-ib -p 5000:5000 -e "MYSQL_PASSWORD=<mysqlPassword>" -e "BASIC_AUTH_USERNAME=<user>" -e "BASIC_AUTH_PASSWORD=<password>" --link ib-mysql:ib-mysql gchevalley/com-stouffcapital-ib`

`docker run -it --rm --name com-stouffcapital-ib -p 5000:5000 --env-file=.env --link ib-mysql:ib-mysql gchevalley/com-stouffcapital-ib`

## Creating The Migration Repository

`flask db init`

### create tables

`flask db migrate -m "init tables"`

`flask db upgrade`

## VBA post

```
Public Function upload_exec(oExec As WebDictionary)

Dim Auth As New HttpBasicAuthenticator
Auth.Setup _
    Username:="<user>", _
    Password:="<password>"

Dim esClient As WebClient
Set esClient = New WebClient
esClient.BaseUrl = "<host>"
Set esClient.Authenticator = Auth


Dim esRequest As WebRequest
Dim esResponse As WebResponse


Set esRequest = New WebRequest
    esRequest.Resource = "/executions"
    esRequest.Method = WebMethod.HttpPost
    esRequest.Format = json
    Set esRequest.Body = oExec


Set esResponse = esClient.Execute(esRequest)
'Debug.Print esResponse.Content

End Function
```

## kubernetes deployment

1. `kubectl create namespace ib`
1. `kubectl -n ib create secret generic ib --from-literal=mysql-password=<pass> --from-literal=backend-user=<user> --from-literal=backend-password=<pass> --from-literal=sentry-sdk=<sentry_sdk>`
1. `kubectl create -f deploy/kubernetes/ib-db-pvc.yaml`
1. `kubectl create -f deploy/kubernetes/ib-sa.yaml`
1. `kubectl create -f deploy/kubernetes/ib-rolebinding.yaml`
1. `kubectl create -f deploy/kubernetes/ib-backend-pvc.yaml`
1. `kubectl create -f deploy/kubernetes/ib-mysql.yaml`
1. `kubectl create -f deploy/kubernetes/ib-backend.yaml`
1. `kubectl create -f deploy/kubernetes/com-stouffcapital-ib-ing-ssl.yaml`

## restore db from mysql pod

`mysql -u ibuser -p ibdb < ib.sql`
