url=""
db="sm_dev"
if [ ! -z $1 ]; then
    url="--url jdbc:mariadb://localhost:3306/$1"
    db="$1"
fi
echo "Creating database and applying schema to '$db'"
echo "CREATE DATABASE IF NOT EXISTS $db" | mysql
liquibase --defaultsFile liquibase-project.properties $url update
