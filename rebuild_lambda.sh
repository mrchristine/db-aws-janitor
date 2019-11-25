#!/bin/bash

case "$1" in
        ec2)
            yes | rm db_aws_ec2_janitor.zip 
            7z a db_aws_ec2_janitor.zip audit_ec2.py ./dep/* aws_janitor config/
            ;;
         
        rds)
            yes | rm db_aws_rds_janitor.zip 
            7z a db_aws_rds_janitor.zip audit_rds.py ./dep/* aws_janitor config/
            ;;

        redshift)
            yes | rm db_aws_redshift_janitor.zip 
            7z a db_aws_redshift_janitor.zip audit_redshift.py ./dep/* aws_janitor config/
            ;;
         
        *)
            echo $"Usage: $0 {ec2|rds|redshift}"
            exit 1
 
esac
