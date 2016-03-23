import logging
import json
import gzip
import datetime
import socket
import ssl
from StringIO import StringIO
from le_config import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)
HOST = 'data.logentries.com'
PORT = 20000


def lambda_handler(event, context):
    # Create socket connection to Logentries
    le_socket = create_socket()

    # get CloudWatch logs
    cw_data = str(event['awslogs']['data'])

    # decode and uncompress CloudWatch logs
    cw_logs = gzip.GzipFile(fileobj=StringIO(cw_data.decode('base64', 'strict'))).read()

    if aws_service == "VPC":
        # convert the log data from JSON into a dictionary
        vpc_event = json.loads(cw_logs)

        # loop through the events line by line
        for t in vpc_event['logEvents']:
            # Transform the data and send it to Logentries
            send_to_le("CEF:0|AWS CloudWatch|FlowLogs|1.0|src=" +
                       str(t['extractedFields']['srcaddr']) +
                       "|spt=" + str(t['extractedFields']['srcport']) +
                       "|dst=" + str(t['extractedFields']['dstaddr']) +
                       "|dpt=" + str(t['extractedFields']['dstport']) +
                       "|proto=" + str(t['extractedFields']['protocol']) +
                       "|start=" + str(t['extractedFields']['start']) +
                       "|end=" + str(t['extractedFields']['end']) +
                       "|out=" + str(t['extractedFields']['bytes']),
                       le_socket)
    le_socket.close()


def create_socket():
    s_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s = ssl.wrap_socket(s_, ca_certs='le_certs.pem', cert_reqs=ssl.CERT_REQUIRED)
    s.connect((HOST, PORT))
    return s


def send_to_le(line, le_socket):
    le_socket.sendall('%s %s\n' % (log_token, line))


