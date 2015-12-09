from botocore.client import Config
from botocore.session import Session
import boto3
import sys
import os

class AWSError(Exception): pass
class AWS_S3Error(AWSError): pass
class AWS_S3COPYError(AWS_S3Error): pass
class AWS_S3DELETEError(AWS_S3Error): pass
class AWS_S3ListError(AWS_S3Error): pass

GENKEY_AES64  = 0
GENKEY_AES128 = 1
GENKEY_AES256 = 2

AES64_BLOCK_SIZE  = 8
AES128_BLOCK_SIZE = 16
AES256_BLOCK_SIZE = 32

def genkey(aes_type):
    if aes_type == GENKEY_AES64 :
        block_size = AES64_BLOCK_SIZE
    elif aes_type == GENKEY_AES128 :
        block_size = AES128_BLOCK_SIZE
    else :
        block_size = AES256_BLOCK_SIZE
    customer_key= os.urandom(block_size)
    return customer_key

class S3Handler(object):
    def __init__(self, proxy=None, proxy_port = None, connect_timeout= 30 , read_timeout= 60):
        if proxy and proxy_port:
            os.environ['HTTP_PROXY'] = 'http://%s:%d' %(proxy, proxy_port)
            os.environ['HTTPS_PROXY'] = 'http://%s:%d' %(proxy, proxy_port)
        config = Config(connect_timeout= connect_timeout , read_timeout= read_timeout, region_name = 'us-west-2')
        session = Session()
        self.conn = boto3.client('s3' , config = config )

    def check_resp_has_lost_structure(self, resp, check_structure):
        lost_structure = {}
        has_lost = False
        if not type(resp) == dict :
            return (True, check_structure)
        for key in check_structure.keys():
            if key in resp:
                if type(resp[key]) == dict and type(check_structure[key]) == dict :
                    (child_has_lost, child_lost_struct) = self.check_resp_has_lost_structure(resp[key], check_structure[key])
                    if child_has_lost:
                        lost_structure[key] = child_lost_struct
                        has_lost = True
        return (has_lost, lost_structure)                     

    def cp_local_file_to_s3(self, bucket_name, src_path, dst_key , customer_sse_key=None , encrypt_algm='AES256', kwargs={}):
        if not bucket_name or not src_path or not dst_key:
            raise AWS_S3COPYError('config error') 
        kwargs['Bucket'] = bucket_name
        kwargs['Key'] = dst_key
        if encrypt_algm and customer_sse_key :
            kwargs['SSECustomerAlgorithm'] = encrypt_algm 
            kwargs['SSECustomerKey'] = customer_sse_key
        else :
            kwargs['ServerSideEncryption'] = 'AES256' # default SSE algorithm
        try:
            with open(src_path , 'rb') as data:
                kwargs['Body'] = data
                resp = self.conn.put_object(**kwargs)
                check_structure = {}
                check_structure['ResponseMetadata'] = {}
                check_structure['ResponseMetadata']['HTTPStatusCode'] = ''
                (has_lost, lost_struct) = self.check_resp_has_lost_structure(resp, check_structure)
                if has_lost:
                    raise Exception('S3 response lost fields. Response body: %s. Lost fields: %s' %(resp, lost_struct))
                if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                    raise Exception('S3 Response Error status. Response Body: %s' %resp)
        except Exception as e_msg:
            raise AWS_S3COPYError(e_msg)

    def cp_s3_file_to_local(self, bucket_name, src_path, dst_key , customer_sse_key=None , encrypt_algm='AES256', kwargs={}):
        if not bucket_name or not src_path or not dst_key:
            raise AWS_S3COPYError('config error') 
        kwargs['Bucket'] = bucket_name
        kwargs['Key'] = dst_key
        if encrypt_algm and customer_sse_key :
            kwargs['SSECustomerAlgorithm'] = encrypt_algm 
            kwargs['SSECustomerKey'] = customer_sse_key
        try:
            resp = self.conn.get_object(**kwargs)
            check_structure = {}
            check_structure['Body'] = ''
            check_structure['ResponseMetadata'] = {}
            check_structure['ResponseMetadata']['HTTPStatusCode'] = ''
            (has_lost, lost_struct) = self.check_resp_has_lost_structure(resp, check_structure)
            if has_lost:
                raise Exception('S3 response lost fields. Response body: %s. Lost fields: %s' %(resp, lost_struct))
            if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise Exception('S3 Response Error status. Response Body: %s' %resp)
            content = resp['Body'].read() 
            with open(src_path, 'wb') as f:
                f.write("%s" %content)
        except Exception as e_msg:
            raise AWS_S3COPYError(e_msg)

    def cp_s3_file_to_s3(self, bucket_name, src_path, dst_key , customer_sse_key=None , encrypt_algm='AES256', kwargs={}):
        if not bucket_name or not src_path or not dst_key:
            raise AWS_S3COPYError('config error') 
        kwargs['Bucket'] = bucket_name
        kwargs['Key'] = dst_key
        kwargs['CopySource'] = src_path
        if encrypt_algm and customer_sse_key :
            kwargs['SSECustomerAlgorithm'] = encrypt_algm 
            kwargs['SSECustomerKey'] = customer_sse_key
        try:
            resp = self.conn.copy_object(**kwargs)
            check_structure = {}
            check_structure['Body'] = ''
            check_structure['ResponseMetadata'] = {}
            check_structure['ResponseMetadata']['HTTPStatusCode'] = ''
            (has_lost, lost_struct) = self.check_resp_has_lost_structure(resp, check_structure)
            if has_lost:
                raise Exception('S3 response lost fields. Response body: %s. Lost fields: %s' %(resp, lost_struct))
            if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise Exception('S3 Response Error status. Response Body: %s' %resp)
        except Exception as e_msg:
            raise AWS_S3COPYError(e_msg)

    def del_s3_file(self, bucket_name, dst_key, kwargs = {}):
        if not bucket_name or not dst_key:
            raise AWS_S3DELETEError('config error') 
        kwargs['Bucket'] = bucket_name
        kwargs['Key'] = dst_key
        try:
            resp = self.conn.delete_object(**kwargs)
            check_structure = {}
            check_structure['ResponseMetadata'] = {}
            check_structure['ResponseMetadata']['HTTPStatusCode'] = ''
            (has_lost, lost_struct) = self.check_resp_has_lost_structure(resp, check_structure)
            if has_lost:
                raise Exception('S3 response lost fields. Response body: %s. Lost fields: %s' %(resp, lost_struct))
            if resp['ResponseMetadata']['HTTPStatusCode'] != 204:
                raise Exception('S3 Response Error status. Response Body: %s' %resp)
        except Exception as e_msg:
            raise AWS_S3DELETEError(e_msg)

    def list_bucket_content(self, bucket_name, prefix = None, kwargs = {}):
        file_list = []
        if not bucket_name:
            raise AWS_S3ListError('config error') 
        kwargs['Bucket'] = bucket_name
        if prefix:
            kwargs['Prefix'] = prefix
        try:
            while True:
                resp = self.conn.list_objects(**kwargs)
                check_structure = {}
                check_structure['ResponseMetadata'] = {}
                check_structure['ResponseMetadata']['HTTPStatusCode'] = ''
                (has_lost, lost_struct) = self.check_resp_has_lost_structure(resp, check_structure)
                if 'Contents' in resp:
                    contents = resp['Contents']
                    for content in contents:
                        file_list.append(content['Key'])
                    if 'IsTruncated' in resp and resp['IsTruncated']:
                        # return is truncated and requests objects after 'NextMarker'
                        kwargs['Marker'] = resp['NextMarker']
                        continue
                    else: 
                        break
                else:
                    break
        except Exception as e_msg:
            raise AWS_S3ListError(e_msg) 
        return file_list

if __name__ == '__main__':
    local_path = sys.argv[1]
    remote_filename = sys.argv[2]
    customer_key = genkey(GENKEY_AES256)
    s3 = S3Handler()
#    s3.cp_s3_file_to_s3('test.tmwrs', 'test.tmwrs/test', 'test1', customer_sse_key=customer_key)
    s3.cp_local_file_to_s3('test.tmwrs', local_path, remote_filename, customer_sse_key=customer_key)
#    s3.cp_s3_file_to_local('test.tmwrs', 'setup.cfg1', remote_filename, customer_sse_key=customer_key)
#    s3.del_s3_file('test.tmwrs', remote_filename)


