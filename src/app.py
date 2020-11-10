import json
import urllib.parse
import boto3
from io import BytesIO
from PyPDF2 import PdfFileReader, PdfFileWriter

s3 = boto3.client('s3')

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    input_bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
    processed_bucket = os.env.get('PROCESSED_BUCKET')

def get_object_stream(bucket_name, key):
    s3_bucket = boto3.resource('s3')
    obj = s3_bucket.Object(bucket_name, key)
    fs = obj.get()['Body'].read()
    pdf_file = PdfFileReader(BytesIO(fs))
    return pdf_file
    
def flatten_pdf(key, input_bucket, output_bucket):
    assert(input_bucket != output_bucket)
    tmp_file = './{}'.format(key)
    input_file = get_object_stream(input_bucket, key)
    output_file = PdfFileWriter()
    output_file.addPage(input_file.getPage(0))
    output_file.updatePageFormFieldValues(output_file.getPage(0), data_dict)
    output_stream = open(tmp_file, "wb")
    output_file.write(output_stream)
    output_stream.close()
    s3 = boto3.resource("s3")
    s3.meta.client.upload_file(tmp_file, output_bucket, key)
