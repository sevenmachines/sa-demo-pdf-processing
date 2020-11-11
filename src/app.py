import json
import os
import logging
import boto3
import botocore
from io import BytesIO
from PyPDF2 import PdfFileReader, PdfFileWriter
import urllib.parse

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')
s3_bucket = boto3.resource('s3')

def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    input_bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=input_bucket, Key=key)
        logger.info("main: processing {} of type {} in bucket {}.".format(key, response['ContentType'], input_bucket))
    except Exception as e:
        logger.error(e)
        logger.error('main: error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, input_bucket))
        raise e
    output_bucket = os.environ.get('PROCESSED_BUCKET')
    process_pdf(key, input_bucket, output_bucket)

def parse_path(s3_key):
    s3_split = s3_key.split('/')
    filename = s3_split[-1]
    if len(s3_split) > 0:
        owner = s3_split[0]
    else:
        owner = None
    return owner, filename

def process_pdf(key, input_bucket, output_bucket):
    assert(input_bucket != output_bucket)
    obj = s3_bucket.Object(input_bucket, key)
    file_path = obj.get()['Body'].read()
    input_stream = get_pdf_reader(key, file_path)
    assert(input_stream is not None)
    files = split_pdf(key, input_stream)
    for tmpkey, outkey in files.items():
        write_pdf(tmpkey, output_bucket, outkey)

def create_tmp_dir(key, tmpdir='/tmp'):
    from pathlib import Path
    outdir = os.path.dirname(key)
    outpath = tmpdir + '/' + outdir
    outkey = tmpdir + '/' + key
    Path(outpath).mkdir(parents=True, exist_ok=True)
    return outpath, outkey

def split_pdf(key, input_stream, max_pages=3):
    files = {}
    count = min(max_pages, int(input_stream.numPages))
    for i in range(count):
            output = PdfFileWriter()
            output.addPage(input_stream.getPage(i))
            outkey = key[:-4]+"-page"+str(i)+".pdf"
            tmpdir, tmpkey = create_tmp_dir(outkey, tmpdir='/tmp') 
            logger.info("split_pdf: writing out {}: {}".format(i, tmpkey))
            with open(tmpkey, "wb") as tmpfile:
                output.write(tmpfile)
                files[tmpkey] = outkey
    return files

def get_pdf_reader(key, file_path):
    reader = PdfFileReader(BytesIO(file_path))
    if reader.isEncrypted:
        owner, filename = parse_path(key)
        secret_id = os.environ.get('ENCRYPTION_KEY_BASE') + '/' + owner
        client = boto3.client('secretsmanager')
        try:
            response = client.get_secret_value(
                SecretId=secret_id,
                VersionStage='AWSCURRENT',
            )
            password = json.loads(response.get('SecretString', {})).get('password', '')
            logger.info("get_pdf_reader: decrypting {} using {} password".format(key, secret_id))
            reader.decrypt(password)
        except botocore.exceptions.ClientError  as e:
            logger.error(e)
    return reader

def write_pdf(filename, output_bucket, key):
    s3 = boto3.resource("s3")
    logger.info("write_pdf: uploading {} to s3://{}/{}.".format(filename, output_bucket, key))
    s3.meta.client.upload_file(filename, output_bucket, key)