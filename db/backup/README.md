# Database Disaster Recovery

## Current Systems

### Daily Backup

A cron job runs a [backup script](https://github.com/populationgenomics/sample-metadata/blob/dev/db/backup/backup.py) daily. The script outputs a folder that is uploaded to GCS in the [cpg-sm-backups](https://console.cloud.google.com/storage/browser/cpg-sm-backups;tab=objects?forceOnBucketsSortingFiltering=false&project=sample-metadata&prefix=&forceOnObjectsSortingFiltering=false) bucket.

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

## DR Validation

### Validation Plan

#### Bi-Annual Procedures

To test our backup procedure, the validate_backup script will be run twice a year. All tests must pass. In the case that tests fail, an investigation should take place.

This includes:

- Identify which tests have failed
- Determine the cause of the failure i.e. the validation script itself, or the backup script.
- Make appropriate changes, and repeat the process until all tests pass.

#### Yearly Procedures

To test our monitoring and alerting policy, once a year our database backups will be disabled for 24 hours. In the case that an alert isn’t triggered, an investigation should take place. In the case of failure, this activity should be repeated within 7 days to ensure relevant changes have taken effect.

Further, to test the effectiveness of our DR procedure, once a year the sm_db_dev_instance will be deleted and restored.

- [Copy the current VM instance](https://cloud.google.com/compute/docs/instances/create-vm-from-similar-instance), creating a new instance (VM_COPY), as a safety measure.
- Update the db-validate-backup secret to reassign the `p_host` field to match the address of VM_COPY. This is for testing purposes.
- Delete `sm_db_dev_instance`
- Create a new prod instance (VM_PROD) and restore in line with the [recovery procedures](#Recovery).
- Validate the new instance in line with the [validation procedures](#Set-Up).
- Test the SM API. #TODO -
- Following successful validation, update the db-validate-backup secret to reassigne the `p_host` field to match the address of VM_PROD.
- Delete VM_COPY.

### Validation Procedure

#### Set-Up

IMPORTANT: Do not run the validation script in a production environment. In order to run the script, all databases on the VM must be dropped.

1. Use a VM with MariaDB 10.5 installed. For instructions, see [Install MariaDB 10.5](#Install-MariaDB-10.5) in the recovery procedures.
2. In the production instance of the database, create a user that has read access to all of the tables.
3. In the Secret Manager, create a secret `db-validate-backup`, with a JSON config as follows, where p_username and p_password matches the user created in step 2 above.

```json
{
  "Dbname": "sm_production",
  "P_host": "sm-db-vm-instance.australia-southeast1-b.c.sample-metadata.internal",
  "P_username": "backup",
  "p_password": "example_password123",
  "l_host": "localhost",
  "L_username": "root",
  "L_password": ""
}
```

#### Running the validation script

```bash
git clone git@github.com:populationgenomics/sample-metadata.git
```

```bash
cd sample-metadata/db/backup
```

```bash
pip3 install -r requirements.txt
```

```bash
python3 -W ignore:ResourceWarning -m unittest validate_backup.py
```

## Recovery Procedures

The full recovery process is detailed below. It is broken up into a series of recovery stages. Follow those that are applicable. It is worth noting that individual tables or databases cannot be restored. In the event that only one table or database has been lost, the entire instance will need to be restored from the last backup point.

### Recovery

1. Restore VM. For instructions on rebuilding the VM [Rebuild VM](#Rebuild-VM). If the VM does not need to be restored skip to step 2.
2. Restore MariaDB Instance. For instructions on installing MariaDB 10.5 the [instructions](#Install-MariaDB-10.5). If MariaDB 10.5 is already running on the VM, skip to 3.
3. Restore the database by running the `restore.py` script

### Rebuild VM

1. Create a new VM instance in the GCP Console.
2. Enable storage write permissions. On the create instance page, scroll down to Access Scopes. Select Set access for each API. Set Storage to Read/Write.
3. Select a service account. The Compute Engine default service account will be selected by default.
4. Connect to your VM

   > ```bash
   > gcloud compute ssh --project=<PROJECT-NAME> >>> --zone=<ZONE> <VM-NAME>
   > ```

5. If you enabled storage write permissions retrospectively, you may encounter `AccessDeniedException: 403 Insufficient Permission` while saving your backups to GCS in later steps. To avoid this, remove the gsutil cache

   > ```bash
   > rm -r ~/.gsutil
   > ```

6. If you forget to enable storage write permissions or change your service account while creating your instance, you can update these settings retrospectively.

   > - View your instance in the GCP Console
   > - Stop your instance
   > - Click Edit

### Install MariaDB 10.5

1. Install MariaDB 10.5. Upon writing this, MariaDB 10.3 is included in the APT package repositories by default, on Debian 10 and Ubuntu 20.4. [The following guide](https://mariadb.com/docs/deploy/upgrade-community-server-cs105-debian9/#install-via-apt-debian-ubuntu) describes the steps involved with configuring the APT package to install 10.5.
2. Install apt-transport-https sudo apt-get install -y apt-transport-https
3. Run the included mysql_secure_installation security script to restrict access to the server. Set up the installation according to your needs. This will allow you to:

   > - Disallow root login remotely
   > - Remove anonymous users
   > - Remove test database and access to it
