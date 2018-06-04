# ib webservice

## set .env file
- SECRET_KEY
- DATABASE_URL="mysql+mysqlconnector://ibuser:<password>@localhost/ibdb"
- BASIC_AUTH_USERNAME=<authUser>
- BASIC_AUTH_PASSWORD=<authPassword>

## mysql db
`docker run --name ibmysql -e MYSQL_RANDOM_ROOT_PASSWORD=yes -e MYSQL_DATABASE=ibdb -e MYSQL_USER=ibuser -e MYSQL_PASSWORD=<password> -p 3306:3306 -d mysql:5.6`


## phpmyadmin
`docker run --name myadmin -d --link ibmysql:db -p 8080:80 phpmyadmin/phpmyadmin`


## Creating The Migration Repository
`flask db init`


### create tables
`flask db migrate -m "init tables"`

`flask db upgrade`
