import logging
import json
import gzip
import re
import socket
import ssl
from StringIO import StringIO
from le_config import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)
HOST = 'data.logentries.com'
PORT = 20000


def lambda_handler(event, context):
    # validate and store debug log tokens
    tokens = []
    if validate_uuid(debug_token) is True:
        tokens.append(debug_token)
    if validate_uuid(lambda_token) is True:
        tokens.append(lambda_token)
    else:
        pass

    # Create socket connection to Logentries
    sock = create_socket()

    # get CloudWatch logs
    cw_data = str(event['awslogs']['data'])

    # decode and uncompress CloudWatch logs
    cw_logs = gzip.GzipFile(fileobj=StringIO(cw_data.decode('base64', 'strict'))).read()

    # convert the log data from JSON into a dictionary
    log_events = json.loads(cw_logs)

    for token in tokens:
        send_to_le("le_cloudwatch \"user\": \"{}\" started sending logs".format(username), sock, token)

    # loop through the events and send to Logentries
    for log_event in log_events['logEvents']:

        # look for extracted fields, if not present, send plain message
        try:
            send_to_le(json.dumps(log_event['extractedFields']), sock, log_token)
        except KeyError:
            for token in tokens:
                send_to_le("le_cloudwatch \"user\": \"{}\" \"key\": \"extractedFields\" not found, "
                           "sending plain text instead. "
                           "Please configure log formats and fields in AWS".format(username), sock, token)
            send_to_le(json.dumps(log_event['message']), sock, log_token)

    # close socket
    for token in tokens:
        send_to_le("le_cloudwatch \"user\": \"{}\" finished sending logs".format(username), sock, token)
    sock.close()


def create_socket():
    s_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s = ssl.wrap_socket(s_, ca_certs='le_certs.pem', cert_reqs=ssl.CERT_REQUIRED)
    s.connect((HOST, PORT))
    return s


def send_to_le(line, le_socket, token):
    le_socket.sendall('%s %s\n' % (token, line))


def validate_uuid(uuid_string):
    regex = re.compile('^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.I)
    match = regex.match(uuid_string)
    return bool(match)
