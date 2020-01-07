from chalice import Chalice, Response
from chalicelib import CONVERT_BINARY_PATH
from subprocess import check_output
import boto3
from json import dumps
from botocore.exceptions import ClientError

app = Chalice(app_name='chalhw')
s3_bucket = 'ul-bagit'

@app.route('/deriv/{params}/{bagname}/{filename}')
def get_deriv(params, bagname, filename):
    orig_file = f"{filename.split('.')[0]}."
    request = app.current_request
    return {'params': params,
            'bagname': bagname,
            'filename': filename,
            'query_params': request.query_params}


@app.route('/recipe/{bagname}')
def get_recipe(bagname):
    # TODO: 
    return {'recipe': bagname}


@app.route('/listfiles/{bagname}')
def list_files(bagname):
    """ This takes a bagname and lists the files in that bag 
        If limit is specified, limiting the results to the specified comma seperated file extension(s)
    """
    params = app.current_request.query_params
    limits = params.get("limit", "").split(',')
    # TODO: query S3 bucket returning list of files limititing to the specified file type
    return  {'bag': bagname, 'file_types': limits}


@app.route('/convert')
def convert():
    # convert is the imagemagick binary
    # TODO: look at lambda layers to see if there an existing one with imagemagick
    output = check_output([CONVERT_BINARY_PATH, '-version'])
    return {'result': dumps(str(output))}


@app.route('/deriv_bag_exists/{bagname}')
def does_deriv_bag_exist(bagname):
    s3_client = boto3.client('s3')
    try:
        s3_client.get_object(Bucket=s3_bucket, Key=f"source/{bagname}/bagit.txt")
        # if we get here then we have access to this item
        return {"exists": True}
    except ClientError:
        return {"exists": False}


@app.route('/serve_deriv/{bagname}/{params}/{filename}')
def serve_deriv(bagname, params, filename):
    s3_client = boto3.client('s3')
    file_key = f"derivative/{bagname}/{params}/data/{filename}"
    try:
        s3_client.get_object(Bucket=s3_bucket, Key=file_key)
        # if we get here then we have access to this item
        return Response(
            status_code=301,
            body='',
            headers={'Location': f"https://bag.ou.edu/{file_key}"}
        )
    except ClientError:
        return Response(status_code=404, body='File not found')
