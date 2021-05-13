import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError


def is_pvc(tag_list):
    if not tag_list:
        return False
    hit_keys = 0
    # pvc_tags = ['KubernetesCluster', 'aws:cloudformation:stack-name']
    for tag in tag_list:
        tag_key = tag.get('Key', None)
        # if both tags exist, then it must be PVC instance
        if tag_key == 'KubernetesCluster':
            hit_keys += 1
        elif tag_key == 'aws:cloudformation:stack-name':
            hit_keys += 1
    if hit_keys == 2:
        return True
    return False


def is_keepalive(tag_list):
    if not tag_list:
        return False
    for tag in tag_list:
        tag_key = tag.get('Key', None)
        if tag_key:
            tag_key_lower = tag_key.lower()
            if tag_key_lower in ['keepalive', 'keep_alive']:
                return True
    return False


def is_databricks_cluster(tag_list):
    if not tag_list:
        return False
    for tag in tag_list:
        tag_key = tag.get('Key', None)
        tag_value = tag.get('Value', None)
        if (tag_key == 'Vendor') and (tag_value == 'Databricks'):
            # Vendor key exists and value is Databricks
            return True
    else:
        return False


def is_running(x_state):
    if x_state.get('Name', None) == 'running':
        return True
    return False

def is_ec2_run_or_stop(x_state):
    # check if the code is running, stopping, or stopped. 
    # https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_InstanceState.html
    if x_state.get('Code', '') in (16, 64, 80):
        return True
    return False

def get_ec2_instance_details(x, region_name='unknown'):
    now = datetime.now(timezone.utc)
    info = {
        'region': region_name,
        'instance_id': x.instance_id,
        'instance_type': x.instance_type,
        'tags': x.tags
        }
    # save all the EC2 properties
    # get instance tags to evaluate if EC2 is outside of Databricks
    info['is_cluster'] = is_databricks_cluster(info['tags'])
    info['is_pvc'] = is_pvc(info['tags'])
    info['is_keepalive'] = is_keepalive(info['tags'])
    info['is_running'] = is_running(x.state)
    # get state information
    info['create_time'] = str(x.launch_time)
    time_delta = now - x.launch_time
    info['runtime'] = str(time_delta)
    info['state'] = x.state
    info['state_reason'] = x.state_reason
    info['runtime_days'] = time_delta.days
    return info


def terminate_instance_ids(i_ids_list, region='us-west-2'):
    # AWS instance filter API returns all instances if empty list is provided!!
    # Need to EXPLICITLY return if empty list is provided
    if not i_ids_list:
        print("Emtpy Instance id list.")
        return []
    
    ec2 = boto3.resource('ec2', region_name=region)
    ec2_client = boto3.client('ec2', region_name=region)
    # flag to determine if all instances have been terminated
    is_instances_alive = True
    # loop until flag is set to False that all termination requests were successful
    while is_instances_alive:
        try:
            ret_term = ec2.instances.filter(InstanceIds=i_ids_list).terminate()
            is_instances_alive = False
        except ClientError as e:
            # catch exception if api termination is disable on the EC2 and reset the attr
            print(e)
            words = str(e).split(' ')
            for w in words:
                if w.startswith("\'i-"):
                    i_id = w.lstrip("\'").rstrip("\'")
                    print("Instance ID to reset: {0}".format(i_id))
                    ec2_client.modify_instance_attribute(InstanceId=i_id, DisableApiTermination={'Value': False})
                    print('Reset disableApiTermination for instance')
