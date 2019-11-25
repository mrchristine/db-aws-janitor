# AWS Janitor Project
Project to audit and log AWS resources to validate policies of tagging and automate termination of resources. 

Environment:
```Python 3.6.8 :: Anaconda custom (x86_64)```

Build a Python package to run against AWS Lambda:
1. Define required python modules in the requirements.txt file
2. `setup.cfg` is needed to download required modules using the download dependency script
3. To package, run `./download_deps.sh` to download modules to the `./dep/` folder to package into a zip later on
4. Run `./rebuild_lambda {ec2 | rds | redshift}` to rebuild the zip files
5. Deploy the zip to lambda

Configurations are kept in a separate folder that is configurable in the common tools module. 
Place a `config/job.conf` file in this project to set up credentials to monitor these services. 
The file content would look like a json object with the following fields:
```
{"account_id": 123456789, "s3_bucket": "my-aws-logs'"}
```


*AWS Lambda Tips*:
1. `boto, requests` pypi modules are included in lambda operations by default, do not package into the zip
2. Local files can only be written to `/tmp`
3. Verify the Role Used has appropriate permissions for the services you want to monitor
4. Use a restrictive IAM role to log to S3. Update the S3 permissions and bucket names properly 

