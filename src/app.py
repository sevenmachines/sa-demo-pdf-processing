import json
import urllib.parse
import boto3
from io import BytesIO
import os
from PyPDF2 import PdfFileReader, PdfFileWriter

s3 = boto3.client('s3')
s3_bucket = boto3.resource('s3')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    input_bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=input_bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, input_bucket))
        raise e
    output_bucket = os.environ.get('PROCESSED_BUCKET')
    process_pdf(key, input_bucket, output_bucket)

    
def process_pdf(key, input_bucket, output_bucket):
    assert(input_bucket != output_bucket)
    obj = s3_bucket.Object(input_bucket, key)
    fs = obj.get()['Body'].read()
    input_stream = PdfFileReader(BytesIO(fs))
    assert(input_stream is not None)
    files = split_pdf(key, input_stream)
    for filename, key in files.items():
        write_pdf(filename, output_bucket, key)

def first_page(key, input_stream,):
    files = {}
    output = PdfFileWriter()
    output.addPage(input_stream.getPage(0))
    with open(filename, "wb") as outfile:
        output.write(outfile)
        files[outfile] = key
    return files

def ensure_decrypt(key, pdf_reader):
    if pdf_reader.is_encrypted():
        secret_id = os.environ.get(ENCRYPTION_KEY_BASE + '/' + key)
        client = boto3.client('secretsmanager')
        response = client.get_secret_value(
            SecretId=secret_id,
            VersionStage='AWSPREVIOUS',
        )           
        password = response.get('SecretString', None)
        pdf_file.decrypt(password)
    return pdf_file

def split_pdf(key, input_stream, max_pages=3):
    files = {}
    tmpdir = '/tmp/'
    count = min(max_pages, int(input_stream.numPages))
    for i in range(count):
            output = PdfFileWriter()
            output.addPage(input_stream.getPage(i))
            outkey = key[:-4]+"-page"+str(i)+".pdf"
            name = tmpdir + outkey
            print("Writing out {}: {}".format(i, outkey))
            with open(name, "wb") as outfile:
                output.write(outfile)
                files[outfile] = outkey
    return files

def write_pdf(filename, output_bucket, key):
    s3 = boto3.resource("s3")
    print("Uploading {} to s3://{}/{}.".format(filename, output_bucket, key))
    s3.meta.client.upload_file(tmp_file, output_bucket, key)