# Database Disaster Recovery

This document defines the procedures defined for database recovery, backup and validation. It outlines our current systems in place to ensure the effective restoration of our SM_DB in the case of disaster.

## Table of Contents

1. [Recovery Procedures](#recovery-procedures)
   1. [Recover Sample Metadata Database](#recover-sample-metadata-database)
      1. [Rebuild the VM](#rebuild-database-vm)
      2. [Install MariaDB 10.5](#install-mariadb-105)
2. [Current Systems](#current-systems)
   1. [Daily Backup](#daily-backup)
   2. [Monitoring and Alerts](#monitoring-and-alerts)
3. [Validation](#validation)
   1. [Validation Procedure](#validation-procedure)
   2. [Validation Plan](#validation-plan)

## Recovery Procedures

The full recovery process is detailed below. Follow the recovery stages that are applicable. It is worth noting that individual tables or databases cannot be restored. In the event that only one table or database has been lost, the entire instance will need to be restored from the last backup point.

### Recover Sample Metadata Database

1. If the `sm-dm-vm-instance` needs to be restored, create a new instance from the [sm-db-vm-image](https://console.cloud.google.com/compute/machineImages/details/sm-dm-vm-image?project=sample-metadata). If you can't access the image, you can rebuild the instance from scratch. See [Rebuild Database VM](#rebuild-database-vm) for further instructions.
2. Validate that MariaDB 10.5 is running on the vm.

   > ```bash
   > > mariadb --version
   > ```
   >
   > If MariaDB 10.5 is not installed, see [Install-MariaDB-10.5](#install-mariadb-105)

3. Clone this repo

   > ```bash
   > git clone https://github.com/populationgenomics/metamist.git
   > ```

4. Navigate to the appropriate directory

   > ```bash
   > cd sample-metadata/db/backup
   > ```

5. Restore the database

   > ```bash
   > python3 restore.py
   > ```

#### Rebuild Database VM

1. Create a new VM instance in the GCP Console.
   1. Enable storage write permissions. On the create instance page, scroll down to Access Scopes. Select Set access for each API. Set Storage to Read/Write.
   2. Select the `sm-db-sa@sample-metadata.iam.gserviceaccount.com` service account.
2. Connect to your VM

   > ```bash
   > gcloud compute ssh --project=<PROJECT-NAME> >>> --zone=<ZONE> <VM-NAME>
   > ```

3. If you enabled storage write permissions retrospectively, you may encounter `AccessDeniedException: 403 Insufficient Permission` while saving your backups to GCS in later steps. To avoid this, remove the gsutil cache

   > ```bash
   > rm -r ~/.gsutil
   > ```

4. If you forget to enable storage write permissions or change your service account while creating your instance, you can update these settings retrospectively.

   > - View your instance in the GCP Console
   > - Stop your instance
   > - Click Edit

#### Install MariaDB 10.5

1. Install MariaDB 10.5. Upon writing this, MariaDB 10.3 is included in the APT package repositories by default, on Debian 10 and Ubuntu 20.4. [The following guide](https://mariadb.com/docs/deploy/upgrade-community-server-cs105-debian9/#install-via-apt-debian-ubuntu) describes the steps involved with configuring the APT package to install 10.5.
2. Install apt-transport-https sudo apt-get install -y apt-transport-https
3. Run the included mysql_secure_installation security script to restrict access to the server. Set up the installation according to your needs. This will allow you to:

   > - Disallow root login remotely
   > - Remove anonymous users
   > - Remove test database and access to it

## Current Systems

### Daily Backup

A cron job runs a [backup script](https://github.com/populationgenomics/metamist/blob/dev/db/backup/backup.py) daily. The script outputs a folder that is uploaded to GCS in the [cpg-sm-backups](https://console.cloud.google.com/storage/browser/cpg-sm-backups;tab=objects?forceOnBucketsSortingFiltering=false&project=sample-metadata&prefix=&forceOnObjectsSortingFiltering=false) bucket.

All backups will be retained for 30 days in the event that they are deleted.
Setting up
Crontab -e
Add the following

```bash
# Back up the database at 4:59am AEST (18:59 UTC) daily
59 18 * * * <path_to_backup_script>
```

### Monitoring and Alerts

Alerting policies have been configured to detect a failure at several stages within the backup script as well as failure to run the backup script altogether. In either case, the #software-alerts channel will be notified accordingly.

A successful backup consists of two events; the mariabackup script is run without error, and the resultant folder is uploaded to GCS. Each event is logged in projects/sample-metadata/logs/backup_log. An alerting policy, derived from a log-based metric is set up. The policy is triggered if both events have not been logged in the last 24 hours.

Similarly, an alerting policy exists to capture failures within the backup script. In this case, an alerting policy is triggered immediately after an event has been logged.

#### Setting Up

1. [Create a log-based metric](https://cloud.google.com/logging/docs/logs-based-metrics#user-metrics)

   > Monitoring Successful Backups
   >
   > > Filter: logName="projects/sample-metadata/logs/backup_log" AND severity = INFO
   >
   > Catching Failed Backups
   >
   > > Filter: logName="projects/sample-metadata/logs/backup_log" AND severity >= ERROR

2. [Create an alerting policy](https://cloud.google.com/logging/docs/logs-based-metrics/charts-and-alerts#alert-on-lbm) based on the log-based metric.

   > Monitoring Successful Backups Configuration:
   >
   > > Aggregator : `Sum`, Period : `1 day`, Condition Triggers if : `Any time series violates`, condition : `is above`, threshold : `2`, for : `1 minute`
   >
   > Catching Failed Backups Configuration:
   >
   > > Period : `1 minute`, Condition Triggers if : `Any time series violates`, condition : `is above`, threshold : `0`, for : `most recent value`

## Validation

### Validation Procedure

IMPORTANT: Do not run the validation script in a production environment. In order to run the script, all databases on the VM must be dropped.

#### Running the validation script

1. Create a new VM using the [validate-db-backup-machine-image](https://console.cloud.google.com/compute/machineImages/details/validate-db-backup-machine-image?project=sample-metadata).
   - Select the `sm-db-sa` service account, to ensure that this VM will have access to the `db-validate-backup` secret.
   - Alternatively, if you wish to select a different service account, ensure that it has access to view the `db-validate-backup` secret in the `sample-metadata` project.
2. Clone this repo

   > ```bash
   > git clone https://github.com/populationgenomics/metamist.git
   > ```

3. Navigate to the directory

   > ```bash
   > cd sample-metadata/db/backup
   > ```

4. Ensure all the dependencies are installed

   > ```bash
   > pip3 install -r requirements.txt
   > ```

5. Run the validation script

   > ```bash
   > python3 -W ignore:ResourceWarning -m unittest validate_backup.py
   > ```

#### First Time Set-Up

1. Use a VM with MariaDB 10.5 installed. For instructions, see [Install MariaDB 10.5](#install-mariadb-105) in the recovery procedures.
2. In the production instance of the database, create a user that has read access to all of the tables.
3. In the Secret Manager, create a secret `db-validate-backup`, with a JSON config as follows, where p_username and p_password matches the user created in step 2 above.

```json
{
  "Dbname": "sm_production",
  "P_host": "sm-db-vm-instance.australia-southeast1-b.c.sample-metadata.internal",
  "P_username": "backup",
  "p_password": "example_password123"
}
```

### Validation Plan

#### Bi-Annual Procedures

To test our backup procedure, the validate_backup script will be run twice a year. All tests must pass. In the case that tests fail, an investigation should take place.

This includes:

- Identify which tests have failed
- Determine the cause of the failure i.e. the validation script itself, or the backup script.
- Make appropriate changes, and repeat the process until all tests pass.

#### Yearly Procedures

To test our monitoring and alerting policy, once a year our database backups will be disabled for 24 hours. In the case that an alert isn’t triggered, an investigation should take place. In the case of failure, this activity should be repeated within 7 days to ensure relevant changes have taken effect.

Further, alongside the procedure to validate the [database restoration](#running-the-validation-script), the SM API will be validated.

1. Update the configuration to point to the new VM as the production VM.
2. Run the test script, currently under construction [#35](https://github.com/populationgenomics/metamist/pull/35)
