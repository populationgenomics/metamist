url=""
db="sm_admin"
mysql_host=""
if [ ! -z $1 ]; then
    mysql_host="--host $1"
    url="--url jdbc:mariadb://$1"
fi
echo "CREATE DATABASE IF NOT EXISTS $db" | mysql $mysql_host
liquibase --defaultsFile liquibase-global.properties $url update
