
import boto3
import oracledb
import os

# set all the parameters for the database connection and the objectstore
'''
set PYTHON_CONNECTSTRING=<server/servicename>
set PYTHON_PASSWORD=<oracle password>
set PYTHON_USERNAME=<oracle username>
set OBJ_STOR_ID=<s3 bucket id/username>
set OBJ_STOR_KEY=<s3 bucket key>
'''
endpoint_url='https://nrs.objectstore.gov.bc.ca:443/' # endpoint for S3 Object Storage -- if this isn't specified it will try and go to Amazon S3
bucketname = '<bucket name>'

un = os.environ.get('PYTHON_USERNAME')
print(un)
pw = os.environ.get('PYTHON_PASSWORD')
cs = os.environ.get('PYTHON_CONNECTSTRING')
print(cs)
objid = os.environ.get('OBJ_STOR_ID')
objkey = os.environ.get('OBJ_STOR_KEY')

#Creating Session With Boto3 for Object Storage
session = boto3.Session(
aws_access_key_id=objid,
aws_secret_access_key=objkey
)

#Creating S3 Resource From the Session.
s3 = session.resource('s3', endpoint_url=endpoint_url)

s3_client = boto3.client('s3',endpoint_url=endpoint_url, 
aws_access_key_id=objid,
aws_secret_access_key=objkey
)

my_bucket = s3.Bucket(bucketname)

#Creating the Oracle connection and sql query to get the lobs
with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
    with connection.cursor() as cursor:
        sql = """select sysdate from dual"""
        for r in cursor.execute(sql):
            print(r)
            print("Successfully connected to Oracle Database")

        sql = """select THE.RESULTS_FRPA_ATTACH_CONTENT.RESULTS_FRPA_ATTACHMENT_ID, THE.RESULTS_FRPA_ATTACH_CONTENT.ATTACHMENT_DATA,
              THE.RESULTS_FRPA_ATTACHMENT.ATTACHMENT_NAME,THE.RESULTS_FRPA_ATTACHMENT.MIME_TYPE_CODE
              from THE.RESULTS_FRPA_ATTACH_CONTENT
              inner join THE.RESULTS_FRPA_ATTACHMENT 
              on THE.RESULTS_FRPA_ATTACH_CONTENT.RESULTS_FRPA_ATTACHMENT_ID = THE.RESULTS_FRPA_ATTACHMENT.RESULTS_FRPA_ATTACHMENT_ID"""
        for r in cursor.execute(sql):
            if (r[1]):
                print(r[0], r[2])
                # if Lob field is full create a file in object storage
                txt_data = r[1]

                #delete bucket objects
                response = s3_client.delete_object(Bucket=bucketname,Key=r[2])              
                #print(response)

                object = s3.Object(bucketname, r[2])

                result = object.put(Body=txt_data.read(), Metadata={'RESULTS_FRPA_ATTACHMENT_ID': str(r[0])})
            else:
                print(r[0], "is NOT done")

