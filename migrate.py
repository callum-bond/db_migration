#!/usr/bin/env python3

# need to get:
# kms key
# region of db 
# name of db 

import boto3
import sys
from datetime import datetime
import time as t 
from botocore.config import Config

my_config = Config(
    region_name='us-west-2'
)

client = boto3.client("rds", config=my_config)

timestamp = datetime.now().strftime("%Y%m%d")

def create_snapshot():
    try:
        response = client.describe_db_instances()
        print("Creating snapshots...")
        print("~" * 20)
        for db_instance in response["DBInstances"]:
            db_instance_name = db_instance["DBInstanceIdentifier"]
            db_snapshot_name = db_instance_name + "-" + timestamp
            if "muttley" in db_instance_name: # to be removed
                if "replica" not in db_instance_name:
                    if "aurora" not in db_instance_name:
                        response = client.create_db_snapshot(
                            DBInstanceIdentifier=db_instance_name,
                            DBSnapshotIdentifier=db_snapshot_name
                        )
                        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                            print("Successfully created DB snapshot.")
                        else:
                            print("Couldn't create DB snapshot.")
    
                        print("Waiting for snapshot(s) to become ready.")
                        number_of_retries = 25
                        snapshot_success = False

                        for i in range(number_of_retries):
                            t.sleep(30)
                            snap_status = client.describe_db_snapshots(
                                DBSnapshotIdentifier=db_snapshot_name # its getting the replica one here
                            )["DBSnapshots"][0]["Status"]
                            if snap_status == "available":
                                snapshot_success = True
                                print("DB snapshot(s) completed.")
                                rename_databases()
                                encrypt_snapshots()
                                break
                            else:
                                print(
                                    "DB snapshot %s is initializing." % (db_snapshot_name)
                                )
                        assert snapshot_success, "DB snaphot %s failed to initialize" % db_snapshot_name
    except Exception as e:
        print(e)

def rename_databases():
    try:
        print("Renaming unencrypted databases...")
        response = client.describe_db_instances()
        for db_instance in response["DBInstances"]:
            db_instance_name = db_instance["DBInstanceIdentifier"]
            db_instance_old = db_instance_name + "-old"
            if "muttley" in db_instance_name: # to be removed
                response = client.modify_db_instance(
                    DBInstanceIdentifier=db_instance_name,
                    NewDBInstanceIdentifier=db_instance_old,
                    ApplyImmediately=True
                )
    except Exception as e:
        print(e)

def encrypt_snapshots():
    print("Encrypting snapshots...")
    try:
        response = client.describe_db_snapshots(
            SnapshotType="Manual"
            )
        for db_snapshot in response["DBSnapshots"]:
            curr_db_snapshot = db_snapshot["DBSnapshotIdentifier"]
            t.sleep(10)
            new_db_snapshot = curr_db_snapshot + "-encrypted"
            response = client.copy_db_snapshot( 
                SourceDBSnapshotIdentifier=curr_db_snapshot,
                TargetDBSnapshotIdentifier=new_db_snapshot,
                KmsKeyId="87bf2f9b-ba23-4aa6-aa34-f3de12f854d0" #'d15d4547-0061-436d-923b-43a57217ef93'
            )
        print("Waiting for encrypted snapshots to become ready..")
        number_of_retries = 25
        snapshot_success = False
        for i in range(number_of_retries):
            t.sleep(30)
            snap_status = client.describe_db_snapshots(
                DBSnapshotIdentifier=new_db_snapshot
            )["DBSnapshots"][0]["Status"]
            if snap_status == "available":
                snapshot_success = True
                print("DB snapshot(s) completed.")
                restore_database()
                break
    except Exception as e: 
        print(e)

def restore_database(): 
    try:
        response = client.describe_db_snapshots(
            SnapshotType="Manual"
            )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            for db_snapshot in response["DBSnapshots"]:
                db_restored_name = db_snapshot["DBSnapshotIdentifier"]
                db_name_trimmed = db_restored_name[:-19]
                if "encrypted" in db_restored_name:
                    if "muttley" in db_restored_name:
                        restore_db_response = client.restore_db_instance_from_db_snapshot(
                            DBInstanceIdentifier = db_name_trimmed,
                            DBSnapshotIdentifier = db_restored_name
                        )
    except Exception as e:
        print(e)
            
    print("Waiting for restored db %s to become ready" % db_name_trimmed)
    number_of_retries = 20
    restore_success = False
    for i in range(number_of_retries):
        t.sleep(30)
        restored_status = client.describe_db_instances(
            DBInstanceIdentifier=db_name_trimmed
        )["DBInstances"][0]["DBInstanceStatus"]
        if restored_status == "available":
            restore_success = True
            print("Restored DB %s is ready" % db_name_trimmed)
            print("Restoring read replicas...")
            response = client.create_db_instance_read_replica(
                DBInstanceIdentifier = db_name_trimmed + "-replica",
                SourceDBInstanceIdentifier = db_name_trimmed,
                KmsKeyId="87bf2f9b-ba23-4aa6-aa34-f3de12f854d0"
            )
            break
        else:
            print("Restored DBs are initializing. Attempt %s" % (i))

    assert restore_success, "Restored DB %s to initialize" % db_restored_name

print("Databases restored.")

if __name__ == "__main__":
    create_snapshot()