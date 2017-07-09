#!/usr/bin/env python
import ConfigParser
import argparse
import logging
import json
import requests
import urllib2, urllib
import sys, os
import boto3
import psycopg2
import csv
import re
import string
import json
import gzip
import traceback
import datetime, time
import dateutil.parser
import errno
import hashlib

def getInt32Le(s):
    b0 = toSigned32(ord(s[0]))
    b1 = toSigned32(ord(s[1]) << 8)
    b2 = toSigned32(ord(s[2]) << 16)
    b3 = toSigned32(ord(s[3]) << 24)
    value = ord(s[0]) ^ (ord(s[1]) << 8) ^ (ord(s[2]) << 16) ^ (ord(s[3]) << 24)
    return toSigned32(value)

def toSigned32(n):
    n = n & 0xffffffff
    return n | (-(n & 0x80000000))

# Ref. https://github.com/digitalbazaar/forge/blob/80c7fd4e21ae83fa236ebb6a2f4748d54aa0dec0/lib/util.js
# Ref. https://github.com/ComparetheMarket/platform.variant-assignment/blob/master/app/strategies/mvt-strategy/index.js
# Ref. https://github.com/ComparetheMarket/platform.variant-assignment/blob/master/app/strategies/mvt-strategy/lib/allocation-calculator.js

def allocateVariant(variantWeightings, contextId, stringifiedToken):
    # concate the contextId & token
    entropy = contextId + stringifiedToken

    # Hash them using MD5
    md = hashlib.md5()
    md.update(entropy)

    # Convert result of hash into long
    location = getInt32Le(md.digest())

     # divide the max long up by the variant weightings
    lastBoundary = -1 * pow(2, 31);
    maxValue = pow(2, 31);
    selectedVariantId = None

    for vw in variantWeightings:
        size = maxValue * (vw['weight'] / 100.0) * 2.0
        lastBoundary += size
        if location <= lastBoundary:
            selectedVariantId = vw['variantId']
            break
    return selectedVariantId

def isInFallowGroup(email):
    token = {"email": email}
    fallowVariantId = '33dea16a-c8d1-4fd9-9dd8-795b82df9e8a';

    weightings = [
        {'weight': 1, 'variantId': fallowVariantId},
        {'weight': 99, 'variantId':  'e29c171a-bbfa-4915-84a6-444ce94a0d3d'},
    ]

    stringifiedToken = json.dumps(token, separators=(',', ':'))
    winningVariant = allocateVariant(weightings, fallowVariantId, stringifiedToken)
    return (winningVariant == fallowVariantId)

def slack_logger(config, msg):
    global slack_ts
    webhook = config.get('Slack', 'webhook')
    channel = config.get('Slack', 'channel')
    enabled = config.get('Slack', 'enabled')
    url_path = "https://slack.com/api/chat.postMessage?"
    token = "xoxp-2335213356-28566503431-140613675585-bcac7e6c2d98017a8135516a769bcab0"

    if enabled == 'true':
        if slack_ts == None:
            values = {"icon_emoji": ":robot_face:",
                      "username": "bigdata-bot",
                      "text": msg,
                      "channel":channel,
                      "token": token,
                      "pretty":"1"}

            data = urllib.urlencode(values)
            server_request = urllib2.Request(url_path + data)
            server_request.add_header('Content-type', 'Accept: application/json')
            server_response = urllib2.urlopen(server_request)
            slack_ts = json.loads(server_response.read())["message"]["ts"]
        else:
            values = {"icon_emoji": ":robot_face:",
                      "username": "bigdata-bot",
                      "text": msg,
                      "channel": channel,
                      "token": token,
                      "pretty": "1",
                      "thread_ts": slack_ts}
            data = urllib.urlencode(values)
            server_request = urllib2.Request(url_path+data)
            server_request.add_header('Content-type', 'Accept: application/json')
            server_response = urllib2.urlopen(server_request)

def create_logger(name, filename):
    logger = logging.getLogger(name)
    formatter_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(filename)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(formatter_string)
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

class Sandstorm(object):
    def __init__(self, config, logger, timestamp):
        self.configuration = config
        self.logger = logger
        self.variant_dict = self.load_variants(config.get('App','variants'))
        self.logger.info("Loading variants")
        self.current_timestamp = timestamp

    def config(self, category, name):
        return self.configuration.get(category, name)

    def execute_redshift_script(self, script, script_name):
        db_name = self.config("Redshift", "db_name")
        db_user = self.config("Redshift", "db_user")
        db_pass = self.config("Redshift", "db_pass")
        db_port = self.config("Redshift", "db_port")
        db_host = self.config("Redshift", "db_host")

        conn = None
        rowcount = 0
        result = None

        try:
            conn = psycopg2.connect(host = db_host,
                                    dbname = db_name,
                                    user = db_user,
                                    password = db_pass,
                                    port = db_port)
            cursor = conn.cursor()
            cursor.execute(script)
            rowcount = cursor.rowcount
            conn.commit()
            cursor.close()
            result = {'status': "SUCCESS", 'rowcount': rowcount}
        except psycopg2.DatabaseError, exception:
            msg = traceback.format_exc()
            self.logger.error("Error: %s" % msg)
            if conn:
                conn.rollback()
                result = {'status':"FAILURE",
                          'timestamp': str(datetime.now()),
                          'script': script_name,
                          'traceback': msg,
                          'exception': exception}
        finally:
            if conn:
                conn.close()
                self.logger.info("Result: %s" % result)
                self.logger.info("Finished running sql script")
                return result

    def download_dir(self, client, resource, dist, local='/tmp', bucket='your_bucket'):
        paginator = client.get_paginator('list_objects')
        for result in paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=dist):
            if result.get('CommonPrefixes') is not None:
                for subdir in result.get('CommonPrefixes'):
                    self.download_dir(client, resource, subdir.get('Prefix'), local, bucket)
            if result.get('Contents') is not None:
                    for file in result.get('Contents'):
                        if not os.path.exists(os.path.dirname(local + os.sep + file.get('Key'))):
                            os.makedirs(os.path.dirname(local + os.sep + file.get('Key')))
                        resource.meta.client.download_file(bucket, file.get('Key'), local + os.sep + file.get('Key'))

    def make_sure_path_exists(self, path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

    def upload_output(self,
                      s3_output_bucket,
                      s3_output_key,
                      local_file_name):
        client = boto3.client('s3')
        resource = boto3.resource('s3')
        base_file_name = os.path.basename(local_file_name)
        s3_target_key = os.path.join(s3_output_key, base_file_name)
        # JKK: added set ACL below so we have the more permissive permisions for the bucket
        # JKK: using a canned ACL ...
        s3_target_key.set_canned_acl('bucket-owner-full-control')
        self.logger.info("Upload " + local_file_name + " file to s3://" + s3_output_bucket + "/" + s3_output_key)
        client.upload_file(local_file_name, s3_output_bucket, s3_target_key, ExtraArgs = {'ServerSideEncryption': "AES256"})
        
    def download_input(self,
                       s3_input_bucket,
                       s3_input_key,
                       input_local,
                       input_dir):
        self.make_sure_path_exists(input_local)

        client = boto3.client('s3')
        resource = boto3.resource('s3')

        self.logger.info("Downloading input data s3://" + s3_input_bucket + "/" + s3_input_key + " to " + input_local)
        self.download_dir(client, resource, dist = s3_input_key, local = input_local, bucket = s3_input_bucket)

    def remove_spaces(self, s):
        return s.translate(None, string.whitespace)

    def parse_contexts(self, contexts):
        return map((lambda x: self.remove_spaces(x)), contexts.split(","))

    def addToDict(self, variant_dict, customer_type, value):
        if not customer_type in variant_dict:
            variant_dict[customer_type] = []
        variant_dict[customer_type].append(value)

    def sandstorm_json(self, email, actions, update_date_iso):
        return json.dumps({"token": {"email": email},
                           "occuredAt": update_date_iso,
                           "actions": actions})

    def load_variants(self, variant_file_name):
        variant_dict = dict()
        with open(variant_file_name, 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in reader:
                customer_type = self.remove_spaces(row[0])
                description = row[1]
                contexts = self.parse_contexts(row[2])
                variant_id = self.remove_spaces(row[3])
                priority = self.remove_spaces(row[4])
                validFrom = self.remove_spaces(row[5])
                expiresAfter =  self.remove_spaces(row[6])
                variant_values = {
                                  "contexts": contexts,
                                  "variantId": variant_id,
                                  "priority": priority,
                                  "validFrom": validFrom,
                                  "expiresAfter": expiresAfter
                                 }
                self.addToDict(variant_dict, customer_type, variant_values)
        return variant_dict

    def convert(self,
                input_dir,
                output_file_name,
                variant_dict,
                s3_fallow_bucket,
                s3_fallow_key,
                fallow_list_file_name):
        all_files = os.listdir(input_dir)

        self.make_sure_path_exists(os.path.dirname(output_file_name))
        self.make_sure_path_exists(os.path.dirname(fallow_list_file_name))

        with gzip.open(fallow_list_file_name, 'wb') as fallowFile:
            with gzip.open(output_file_name, 'wb') as f:
                for file in all_files:
                    input_file_name = os.path.join(input_dir, file)
                    with open(input_file_name, 'rb') as csvfile:
                        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                        for row in reader:
                            email = row[0]
                            customer_type = row[1]
                            update_date = dateutil.parser.parse(row[2]).isoformat()
                            actions = variant_dict[customer_type]
                            if isInFallowGroup(email):
                                fallowFile.write(email)
                                fallowFile.write("\n")
                            else:
                                json_line = self.sandstorm_json(email, actions, update_date)
                                f.write(json_line.encode('utf8'))
                                f.write("\n")

    def output_file_name(self):
        return '{}/{}.jsonl.gz'.format(self.config('Output', 'output_local'), self.current_timestamp['timestamp'])

    def fallow_list_file_name(self):
        return '{}/Fallow-{}.jsonl.gz'.format('fallow', self.current_timestamp['timestamp'])

    def create_unload_script(self, s3_bucket,
                          s3_key,
                          sql_script_name,
                          timestamp):
        with open(sql_script_name, 'r') as template_file:
            template = template_file.read()
            template = template.replace("${YEAR}", timestamp['year'])
            template = template.replace("${MONTH}", timestamp['month'])
            template = template.replace("${DAY}", timestamp['day'])
            template = template.replace("${BATCH_ID}", timestamp['timestamp'])
            template = template.replace("${S3_BUCKET}", s3_bucket)
            template = template.replace("${S3_KEY}", s3_key)

        script = template
        return script

    def run(self):
        print("Sandstorm Extractor Running...")
        timestamp = self.current_timestamp
        sql_script_name = 'sandstorm_unload.sql'
        s3_input_bucket = self.config('Input', 'bucket')
        s3_output_bucket = self.config('Output', 'bucket')
        s3_output_key = self.config('Output', 'output_key')
        s3_input_root_key = self.config('Input', 'input_key')
        s3_fallow_bucket = self.config('Fallow', 'bucket')
        s3_fallow_key = self.config('Fallow', 'key')

        timestamp_suffix = os.path.join(timestamp['year'],
                                        timestamp['month'],
                                        timestamp['day'],
                                        timestamp['timestamp'])
        s3_input_key = os.path.join(s3_input_root_key, timestamp_suffix)
        input_local = self.config('Input', 'input_local')
        input_dir = os.path.join(input_local, s3_input_key)
        output_file_name = self.output_file_name()
        fallow_list_file_name = self.fallow_list_file_name()

        self.logger.info("Timestamp = {}".format(timestamp))
        self.logger.info("Timestamp_suffix = {}".format(timestamp_suffix))
        self.logger.info("s3_input_bucket = {}".format(s3_input_bucket))
        self.logger.info("s3_input_root_key = {}".format(s3_input_root_key))
        self.logger.info("s3_input_key = {}".format(s3_input_key))
        self.logger.info("input_local = {}".format(input_local))
        self.logger.info("input_dir = {}".format(input_dir))
        self.logger.info("s3_output_bucket = {}".format(s3_output_bucket))
        self.logger.info("s3_output_key = {}".format(s3_output_key))
        self.logger.info("output_file_name = {}".format(output_file_name))
        self.logger.info("s3_fallow_bucket = {}".format(s3_fallow_bucket))
        self.logger.info("s3_fallow_key = {}".format(s3_fallow_key))

        # Create unload script using the template
        script = self.create_unload_script(s3_input_bucket,
                                           s3_input_root_key,
                                           sql_script_name,
                                           self.current_timestamp)

        # Run unload script in redshift
        self.logger.info("Running unload script in redshift")
        self.execute_redshift_script(script, sql_script_name)
        self.logger.info("Unload completed")

        # Download data
        self.logger.info("Download data from s3")
        self.download_input(s3_input_bucket,
                            s3_input_key,
                            input_local,
                            input_dir)
        self.logger.info("Download completed")

        # Convert csv to json
        self.logger.info("Converting dir: " + input_dir + " to " + self.output_file_name())
        self.convert(input_dir,
                     output_file_name,
                     self.variant_dict,
                     s3_fallow_bucket,
                     s3_fallow_key,
                     fallow_list_file_name)

        self.logger.info("Conversion completed")

        # Upload file to s3
        self.upload_output(s3_output_bucket,
                           s3_output_key,
                           output_file_name)
        self.logger.info("Upload completed")

        # Upload fallow
        self.upload_output(s3_fallow_bucket,
                           s3_fallow_key,
                           fallow_list_file_name)
        self.logger.info("Fallow Group Upload completed")

def now():
    now = datetime.datetime.now()
    month = '{:02d}'.format(now.month)
    day = '{:02d}'.format(now.day)
    year = '{:d}'.format(now.year)
    hour = '{:02d}'.format(now.hour)
    minute = '{:02d}'.format(now.minute)
    timestamp = "{}{}{}{}{}".format(year, month, day, hour, minute)
    return {'year': year,
            'month': month,
            'day': day,
            'hour': hour,
            'minute': minute,
            'timestamp': timestamp}

def send_logs(config, timestamp):
    s3_bucket = config.get('Log', 'bucket')
    s3_key = config.get('Log', 'log_key')
    log_file_name = config.get('Log', 'local_log')
    s3_log_file_name = 'log-{}.txt'.format(timestamp['timestamp'])
    s3_target_key = os.path.join(s3_key,
                                 timestamp['year'],
                                 timestamp['month'],
                                 timestamp['day'],
                                 s3_log_file_name)
    client = boto3.client('s3')
    resource = boto3.resource('s3')
    client.upload_file(log_file_name, s3_bucket, s3_target_key)
    return 's3://{}/{}'.format(s3_bucket, s3_target_key)

def get_script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Sandstorm Extractor')

    parser.add_argument('--config',
                        metavar='config',
                        help='Specify configuration file',
                        required=True)

    parser.add_argument('--redshift_password',
                        metavar='redshift',
                        help='password to access',
                        required=True)

    args = parser.parse_args()

    if not args.config or not args.redshift_password:
        parser.print_help()
        sys.exit(1)

    configFile = os.path.join(get_script_path(), args.config)

    timestamp = now()
    exit_code = 0
    slack_ts = None

    config = ConfigParser.ConfigParser()
    config.read(configFile)
    config.set("Redshift", "db_pass", args.redshift_password)

    logger = create_logger('Sandstorm', config.get('Log', 'local_log'))

    msg_running   = 'Sandstorm service is running [BATCHID: {}]'
    msg_completed = 'Sandstorm service completed successfully. [BATCHID: {}]'
    msg_exception = 'Sandstorm service `failed with an exception` [BATCHID: {}]'
    msg_terminate = 'Sandstorm service is terminating. [BATCHID: {}] You can find the logs in `{}`'

    try:
        slack_logger(config, msg_running.format(timestamp['timestamp']))
        sandstorm = Sandstorm(config, logger, timestamp)
        sandstorm.run()
        slack_logger(config, msg_completed.format(timestamp['timestamp']))
    except Exception as ex:
        logger.error(traceback.format_exc())
        slack_logger(config, msg_exception.format(timestamp['timestamp']))
        exit_code = -1
    finally:
        s3_log_location = send_logs(config, timestamp)
        slack_logger(config, msg_terminate.format(timestamp['timestamp'], s3_log_location))
        print("OK")
        sys.exit(exit_code)
