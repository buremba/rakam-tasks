## rakam-tasks
Simple event collector functions can be run on AWS Lambda.

## Upload to S3
aws s3 cp out/hurriyet-api.zip s3://rakam-task-market/hurriyet-api.zip --metadata runtime=python2.7,handler=main.fetch,version=0.1,description="Data collector for Hurriyet API"