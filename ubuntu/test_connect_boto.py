import boto
from boto.ec2.regioninfo import RegionInfo
region=RegionInfo(name='tasmania', endpoint='nova.rc.nectar.org.au') 
ec2_conn = boto.ec2.connect_to_region('tasmania',aws_access_key_id='4229d1bba11848adb53230254f1b0004', aws_secret_access_key='01809aa937e6410ebe9d449f44b7f294')
images = ec2_conn.get_all_images()
for img in images:
 print 'id: ', img.id, ', name: ', img.name

