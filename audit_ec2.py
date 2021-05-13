from aws_janitor import *


def audit_ec2(enable_terminate = False):
    # config details
    region_list = get_region_list()

    # logging args
    bucket_name = get_s3_bucket()
    # 2 subdirs in s3 bucket for logging
    folder_name = 'ec2_logs'
    folder_name_excluded = 'ec2_excluded_logs'
    # s3 file logfile prefix
    log_prefix = '{0}_ec2'
    # 2 local logs
    local_log = '/tmp/tmp_ec2.log'
    local_excluded_log = '/tmp/tmp_ec2_excluded.log'
    
    ###################################################################################
    ## Excluded EC2 Instances
    ## Exclude Databricks Clusters w/ tag Vendor
    ## Exclude EC2 instances with tag KeepAlive or keep_alive set to true
    ##################################################################################
    # ec2.instances.filter(InstanceIds=ids).terminate()
    for region in region_list:
        print("Listing clusters for region {0}".format(region))
        ec2 = boto3.resource('ec2', region_name=region)
        instances = ec2.instances.all()
        instance_ids = []
        # count to find num of running ec2 that are not DB clusters
        num_ec2 = 0
        num_ec2_excluded = 0
    
        # check local log file first and remove previous file if exists
        rm_local_log(local_log)
        rm_local_log(local_excluded_log)
    
        # loop over instances to check for criteria
        for x in instances:
            with open(local_log, 'a') as fp, open(local_excluded_log, 'a') as fp_ex:
                x_info = get_ec2_instance_details(x, region_name=region)
                if is_ec2_run_or_stop(x_info['state']):
                    # if not a Databricks cluster, 
                    # if not tagged with keep_alive
                    # log to local file before upload to s3
                    if ((not x_info['is_cluster']) and
                        (not x_info['is_keepalive'])):
                        num_ec2 += 1
                        fp.write(json.dumps(x_info))
                        fp.write("\n")
                        tag_list = x_info.get('tags', None)
                        if tag_list:
                            has_databricks_email = has_databricks_owner_tag(tag_list)
                        else:
                            has_databricks_email = False
                        # delete instances without an owner tag w/ a databricks email
                        if not has_databricks_email:
                            instance_ids.append(x_info['instance_id'])
                        # get the instance ids that have been running for 10 days
                        #if x_info['runtime_days'] > 10:
                        #    instance_ids.append(x_info['instance_id'])
                    else:
                        fp_ex.write(json.dumps(x_info))
                        fp_ex.write("\n")
                        num_ec2_excluded += 1
    
        if num_ec2 > 0:
            with open(local_log, 'r') as fp_read:
                log_to_s3(bucket_name, folder_name, log_prefix.format(region), fp_read.read())
        if num_ec2_excluded > 0:
            with open(local_excluded_log, 'r') as fp_read:
                log_to_s3(bucket_name, folder_name_excluded, log_prefix.format(region), fp_read.read())
        print("Terminating the following instance ids:")
        print(instance_ids)
        if enable_terminate:
            terminate_instance_ids(instance_ids, region)
        print("################################################################################")


def lambda_handler(event, context):
    # False will only log the resources to s3
    # True will terminate instances
    audit_ec2(True)
    message = "Completed audit of EC2 to S3 bucket logs!"
    return {
        'message' : message
    }
