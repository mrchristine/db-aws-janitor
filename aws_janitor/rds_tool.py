import boto3
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


def get_rds_details(x, region_name='unknown'):
    now = datetime.now(timezone.utc)
    # https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Status.html
    # status is not important for our purposes
    info = {
        'region': region_name,
        'db_name': x['DBInstanceIdentifier'],
        'cluster_id': x.get('DBClusterIdentifier', None),
        'node_type': x['DBInstanceClass'],
        'db_type': x['Engine'],
        'status': x['DBInstanceStatus'],
        'tags': x['tags']
    }
    # save all the RDS properties
    # this field is optional and could be missing. see boto3 docs
    if not info['cluster_id']:
        # set the cluster_id to the db_name if empty
        info['cluster_id'] = info['db_name']

    # get rds tags to evaluate if RDS has keep_alive tag
    info['is_keepalive'] = is_keepalive(info['tags'])

    # get state / runtime information
    info['create_time'] = str(x['InstanceCreateTime'])
    time_delta = now - x['InstanceCreateTime']
    info['runtime'] = str(time_delta)
    info['runtime_days'] = time_delta.days
    return info


def terminate_rds_instances(rds_list, region='us-west-2'):
    if not rds_list:
        print("Empty RDS list provided.")
        return []
    client = boto3.client('rds', region_name=region)
    for rds in rds_list:
        print("\nDB NAME: {0}\n".format(rds['db_name']))
        is_rds_alive = True
        while is_rds_alive:
            try:
                del_instance = client.delete_db_instance(DBInstanceIdentifier=rds['db_name'],
                                                         SkipFinalSnapshot=True,
                                                         DeleteAutomatedBackups=True)
                # if the db_name and cluster_id match, then there is only an instance available
                # otherwise its an database composed of instances, which is why we need both deletes
                if rds['db_name'] != rds['cluster_id']:
                    del_db = client.delete_db_cluster(DBClusterIdentifier=rds['cluster_id'], SkipFinalSnapshot=True)

                is_rds_alive = False
            except ClientError as e:
                print(e)
                if "is already being deleted" in str(e):
                    is_rds_alive = False
                else:
                    # RDS is part of a cluster, must disable term protection on cluster
                    if rds['db_name'] != rds['cluster_id']:
                        client.modify_db_cluster(DBClusterIdentifier=rds['cluster_id'], DeletionProtection=False)
                    else:
                        client.modify_db_instance(DBInstanceIdentifier=rds['db_name'], DeletionProtection=False)
