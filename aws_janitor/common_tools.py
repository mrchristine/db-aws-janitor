import boto3, pytz, json, os
from datetime import datetime, timezone


def get_account_id(conf="../config/job.conf"):
    with open(conf, "r") as fp:
        config = json.loads(fp.read())
    return config.get('account_id')


def get_s3_bucket(conf="../config/job.conf"):
    with open(conf, "r") as fp:
        config = json.loads(fp.read())
    return config.get('s3_bucket')


def rm_local_log(fname):
    log_path = os.path.dirname(fname)
    if not os.path.exists(log_path):
        os.makedirs(log_path, exist_ok=True)
    if os.path.isfile(fname):
        os.remove(fname)


def get_region_list():
    ec2 = boto3.client('ec2')
    regions = ec2.describe_regions()
    region_list = [x['RegionName'] for x in regions['Regions']]
    return region_list


def get_current_date_pt():
    # get the current UTC time and do not depend on the TZ of where this is run
    u = datetime.utcnow()
    # use PT due to where HQ is located
    u = u.replace(tzinfo=pytz.utc)
    return u.astimezone(pytz.timezone("America/Los_Angeles")).strftime("%Y-%m-%d")


def log_to_s3(bucket_name, folder, log_prefix, contents):
    date_str = get_current_date_pt()
    runtime_str = datetime.now().strftime("%Y%m%d-%H%M%S")
    fname = "{0}_{1}.json".format(log_prefix, runtime_str)
    full_fname = "{0}/date={1}/{2}".format(folder, date_str, fname)
    s3 = boto3.resource('s3')
    # creating a json file and putting it in the folder
    s3.Object(bucket_name, full_fname).put(Body=contents)
