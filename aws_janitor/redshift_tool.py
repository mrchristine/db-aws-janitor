import boto3, pytz, json
from datetime import datetime, timezone
from botocore.exceptions import ClientError


def is_keepalive(tag_list):
    # set of tags to exclude resources from tools
    # excluded resources are reported to management
    if not tag_list:
        return False
    for tag in tag_list:
        tag_key = tag.get('Key', None)
        if tag_key:
            tag_key_lower = tag_key.lower()
            if tag_key_lower in ['keepalive', 'keep_alive']:
                return True
    return False


def get_redshift_details(x, region_name='unknown'):
    now = datetime.now(timezone.utc)
    info = {}
    # save all the RDS properties
    info['region'] = region_name
    info['cluster_name'] = x.get('ClusterIdentifier', None)
    # this field is optional and could be missing. see boto3 docs
    info['cluster_id'] = x.get('DBClusterIdentifier', None)
    info['node_type'] = x['NodeType']

    # get rds tags to evaluate if RDS has keep_alive tag
    info['tags'] = x['Tags']
    # cluster status shouldn't matter, there's no stopped status for redshift
    info['status'] = x['ClusterStatus']
    info['is_keepalive'] = is_keepalive(info['tags'])

    # get state / runtime information
    info['create_time'] = str(x['ClusterCreateTime'])
    time_delta = now - x['ClusterCreateTime']
    info['runtime'] = str(time_delta)
    info['runtime_days'] = time_delta.days
    return info


def terminate_redshift_instances(redshift_list, region):
    client = boto3.client('redshift', region_name=region)
    for rs in redshift_list:
        print("Terminating {0}".format(rs['cluster_name']))
        resp = client.delete_cluster(ClusterIdentifier=rs['cluster_name'],
                                     SkipFinalClusterSnapshot=True)
        print(resp)
