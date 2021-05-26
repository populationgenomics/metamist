# Getting Started 
## Set Up & Configuring the VM 
1. Create a new VM instance in the [GCP Console](https://console.cloud.google.com/compute/instancesAdd). 
2. Enable storage write permissions. On the create instance page, scroll down to `Access Scopes`. Select `Set access for each API`. Set `Storage` to `Read/Write`.
    
    If you forget to enable storage write permissions while creating your instance, you can update these settings retrospectively.
    1. View your instance in the [GCP Console](https://console.cloud.google.com/compute/instances)
    2. Stop your instance 
    3. Click Edit 
    4. Repeat step 2  


3. Connect to your VM 
`gcloud compute ssh --project=<PROJECT-NAME> --zone=<ZONE> <VM-NAME>` 
    1. If you enabled storage write permissions **retrospectively**, you may encounter `AccessDeniedException: 403 Insufficient Permission` while saving your backups to GCS in later steps. To avoid this, remove the gsutil cache `rm -r ~/.gsutil`

## Installing and Configuring the MariaDB 
1. Install MariaDB 10.5. Upon writing this, MariaDB 10.3 is included in the APT package repositories by default, on Debian 10 and Ubuntu 20.4. [The following guide](https://mariadb.com/docs/deploy/upgrade-community-server-cs105-debian9/#install-via-apt-debian-ubuntu) describes the steps involved with configuring the APT package to install 10.5. 
    1. You may need to also install apt-transport-https
    `sudo apt-get install -y apt-transport-https`
2. Run the included `mysql_secure_installation` security script to restrict access to the server
3. Set up the installation according to your needs;
    This will allow you to
    1. Disallow root login remotely 
    2. Remove anonymous users 
    3. Remove test database and access to it 

## Create Database and New User
1. Connect to the server using the msql client
`sudo mysql -u root`
2. Verify the system version is at least 10.5 
 `SHOW GLOBAL VARIABLES LIKE 'version';`
3. Create a database for your project
`CREATE DATABASE <project>;`
4. Change the current database
    `USE <project>;`
5. Verify that there aren’t any tables at this stage
    `SHOW TABLES;`
6. [Create a new user to access the database and grant privileges](https://phoenixnap.com/kb/how-to-create-mariadb-user-grant-privilege) for the database created above.


## Install Liquibase
The full guide to installing Liquibase can be found [here](https://docs.liquibase.com/concepts/installation/installation-linux-unix-mac.html). Alternatively, you can follow the steps below. 
1. Exit the mysql client using `quit`, or open a new terminal window. 
2. Ensure java is installed. You can check with `java --version`. If it isn’t installed run `sudo apt install default-jre`
3. Download the Liquibase files 
`wget https://github.com/liquibase/liquibase/releases/download/v4.3.5/liquibase-4.3.5.tar.gz`
4. Unzip the file in a directory of your choice 
 `tar -xf liquibase-4.3.5.tar.gz` 
5. Add this directory, containing your unzipped files, to your path 
For example `export PATH=$PATH:/path/to/Liquibase-<version>-bin`
6. Validate the installation `liquibase --version`
 
 
## Deploy Liquibase 
 
1. [Install and Configure Git](https://www.digitalocean.com/community/tutorials/how-to-install-git-on-debian-10)
2. Clone the sample metadata repository `git clone git@github.com:populationgenomics/sample-metadata.git`
3. (TEMPORARY) git checkout add-foundations
4. Enter the database directory `cd db` 
5. Modify liquibase.properties file with your username, password and url determined in step 6 of the Create Database and New User stage. 
6. (TEMP?) Within the same folder, download the MariaDB connector 
 `wget https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/2.5.3/mariadb-java-client-2.5.3.jar`
7. Make the driver executable `chmod +x mariadb-java-client-2.5.3.jar` 
8. `liquibase --changeLogFile 2021-05-10-no-history.xml update`
 
## Setting Up Backups 
1. Create a file called backup_db `touch backup_db`
2. Add the following code to the file, modifying  `[FILENAME_PREFIX]`, `[DATABASE]` and `path/to/backup`. `[DATABASE]` was defined in Step 3 of the Create Database stage. `/path/to/backup` defines a GCS bucket where the backups of your database will be stored. `[FILE_NAMEPREFIX]` is an optional prefix for each backup file. 
`nano backup_db`
~~~
#!/bin/bash
timestamp=`date +%d-%m-%Y_%H-%M-%S`
db=[FILENAME_PREFIX]
file="$db$timestamp"
sudo mysqldump --lock-tables --databases [DATABASE] > $file.sql
gsutil mv $file.sql gs://path/to/backups
~~~
 
3. Make the file executable `chmod + backup_db` 
4. Create a cron job to execute the backup at a specified recurring time. Run `crontab -e` to create a new job. 
    1. If prompted, select an editor that you feel comfortable with 
5. Define your backup job 
~~~
# Example of job definition:
# .---------------- minute (0 - 59)`
# |  .------------- hour (0 - 23)`
# |  |  .---------- day of month (1 - 31)`
# |  |  |  .------- month (1 - 12) OR jan,feb,mar,apr ...`
# |  |  |  |  .---- day of week (0 - 6) (Sunday=0 or 7) OR sun,mon,tue,wed,thu,fri,sat`
# |  |  |  |  |`
# *  *  *  *  * command to be executed`
~~~
 
For example, to back up the database at 4:59 AM AEST add the following line 
~~~
# Back up the database at 4:59am AEST (18:59 UTC) daily 
59 18 * * * backup_db
~~~


