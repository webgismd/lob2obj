# LOB2OBJ 

## Description
This python script pulls lobs out of oracle and puts them into Object Storage S3. 
If the object already exists it will be overwritten. One metadata tag is added - 
x-amz-meta-results_frpa_attachment_id: (which represents the primary key in the oracle database)

## require libraries

* python -m pip install boto3
*  python -m pip install oracledb

## required parameters to set

 set all the parameters for the database connection and the objectstore
'''
* set PYTHON_CONNECTSTRING=<server/servicename>
* set PYTHON_PASSWORD=<oracle password>
* set PYTHON_USERNAME=<oracle username>
* set OBJ_STOR_ID=<s3 bucket id/username>
* set OBJ_STOR_KEY=<s3 bucket key>
'''
* endpoint_url='https://nrs.objectstore.gov.bc.ca:443/' # endpoint for S3 Object Storage -- if this isn't specified it will try and go to Amazon S3
* bucketname = '<bucket name>'
