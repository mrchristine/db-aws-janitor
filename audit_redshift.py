from aws_janitor import *


def audit_redshift(enable_terminate=False):
    # logging args
    bucket_name = get_s3_bucket()
    # 2 subdirs in s3 bucket for logging
    folder_name = 'redshift_logs'
    folder_name_excluded = 'redshift_excluded_logs'
    # s3 file logfile prefix
    log_prefix = '{0}_redshift'
    # 2 local logs
    local_log = '/tmp/tmp_redshift.log'
    local_excluded_log = '/tmp/tmp_redshift_excluded.log'
    
    region_list = get_region_list()
    
    for region in region_list:
        print("Listing clusters for region {0}".format(region))
        client = boto3.client('redshift', region_name=region)
        cl = client.describe_clusters()
        num_x = 0
        num_excluded_x = 0
        x_ids = []
        rm_local_log(local_log)
        rm_local_log(local_excluded_log)
    
        for x in cl['Clusters']:
            with open(local_log, 'a') as fp, open(local_excluded_log, 'a') as fp_ex:
                info = get_redshift_details(x, region)
                if not info['is_keepalive']:
                    num_x+=1
                    fp.write(json.dumps(info))
                    fp.write('\n')
                    # delete instances without an owner tag w/ a databricks email
                    has_databricks_email = has_databricks_owner_tag(info['tags'])
                    if not has_databricks_email:
                        x_ids.append(info)
                    # terminate RDS running for more than 10 days
                    if info['runtime_days'] > 10:
                        x_ids.append(info)
                else:
                    num_excluded_x += 1
                    fp_ex.write(json.dumps(info))
                    fp_ex.write('\n')
                print("Cluster name: {0}\nRuntime: {1}".format(info['cluster_name'], info['runtime']))
                print("Cluster tags: {0}\n".format(info['tags']))
        # log to S3
        if num_x > 0:
            with open(local_log, 'r') as fp_read:
                log_to_s3(bucket_name, folder_name, log_prefix.format(region), fp_read.read())
        if num_excluded_x > 0:
            with open(local_excluded_log, 'r') as fp_read:
                log_to_s3(bucket_name, folder_name_excluded, log_prefix.format(region), fp_read.read())
    
        print(x_ids)
        # pass in a list of rds instance objects and region name
        if enable_terminate:
            terminate_redshift_instances(x_ids, region)
        print("################################################################################")


def lambda_handler(event, context):
    # False will not terminate instances
    # True will shutdown resources over 10 days
    audit_redshift(True)
    message = "Completed audit of Redshift clusters to S3!"
    return {
        'message': message
    }

