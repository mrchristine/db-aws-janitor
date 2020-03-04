from aws_janitor import *
import json


def audit_rds(enable_terminate=False):
    region_list = get_region_list()
    account_id = get_account_id()

    # logging args
    bucket_name = get_s3_bucket()
    # 2 subdirs in s3 bucket for logging
    folder_name = 'rds_logs'
    folder_name_excluded = 'rds_excluded_logs'
    # s3 file logfile prefix
    log_prefix = '{0}_rds'
    # 2 local logs
    local_log = '/tmp/tmp_rds.log'
    local_excluded_log = '/tmp/tmp_rds_excluded.log'

    for region in region_list:
        print("Listing clusters for region {0}".format(region))
        client = boto3.client('rds', region_name=region)
        db_list = client.describe_db_instances().get('DBInstances', None)
        # configure initial settings and reset local logs
        num_rds = 0
        num_excluded_rds = 0
        rds_ids = []
        # check local log file first and remove previous file if exists
        rm_local_log(local_log)
        rm_local_log(local_excluded_log)

        if db_list:
            for x in db_list:
                with open(local_log, 'a') as fp, open(local_excluded_log, 'a') as fp_ex:
                    arn = "arn:aws:rds:{0}:{1}:db:{2}".format(region, account_id, x['DBInstanceIdentifier'])
                    tags = client.list_tags_for_resource(ResourceName=arn).get('TagList', None)
                    x['arn'] = arn
                    x['tags'] = tags
                    # fetch an object to get the RDS properties as a dict
                    rds = get_rds_details(x, region_name=region)
                    if not rds['is_keepalive']:
                        # shutdown resources without keepalive tag set
                        num_rds += 1
                        fp.write(json.dumps(rds))
                        fp.write('\n')
                        has_databricks_email = has_databricks_owner_tag(rds['tags'])
                        # delete instances without an owner tag w/ a databricks email
                        if not has_databricks_email:
                            rds_ids.append(rds)
                        # terminate RDS running for more than 10 days
                        # removing this policy for now
                        #if rds['runtime_days'] > 10:
                        #    rds_ids.append(rds)
                    else:
                        num_excluded_rds += 1
                        fp_ex.write(json.dumps(rds))
                        fp_ex.write('\n')
                    print("RDS Name: {0}\nRuntime: {1}".format(rds['db_name'], rds['runtime']))

        # log to S3
        if num_rds > 0:
            with open(local_log, 'r') as fp_read:
                log_to_s3(bucket_name, folder_name, log_prefix.format(region), fp_read.read())
        if num_excluded_rds > 0:
            with open(local_excluded_log, 'r') as fp_read:
                log_to_s3(bucket_name, folder_name_excluded, log_prefix.format(region), fp_read.read())

        print(rds_ids)
        # pass in a list of rds instance objects and region name
        if enable_terminate:
            terminate_rds_instances(rds_ids, region)
        print("################################################################################")


def lambda_handler(event, context):
    # False will not terminate instances
    # True will shutdown resources over 10 days
    audit_rds(True)
    message = "Completed audit of RDS instances to S3!"
    return {
        'message': message
    }

