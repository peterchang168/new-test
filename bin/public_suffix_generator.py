#!/usr/bin/python2.6
'''
public_suffix_generator download the latest public suffix table from https://publicsuffix.org/ and processed it to WCS internal format
Following is specification of the file:

    The list uses ascii encoding, all non-ascii rules are converted to Punycode form.
    The list is a set of rules, with one rule per line.
    Each line is read up to the new-line character; entire lines can also be commented using //.
    Each line which is not entirely whitespace or begins with a comment contains a rule record.
    Each rule record has 3 fields separated by tab character: rule, flag, threshold.
    Each rule lists a public suffix, with the subdomain portions separated by dots (.) as usual. There is no leading dot.
    The wildcard character * (asterisk) matches any valid sequence of characters in a hostname part.
    Wildcards may only be used to wildcard an entire level. That is, they must be surrounded by dots (or implicit dots, at the beginning of a line).
    (Currently only support wildcards to be placed in first label)
    If a hostname matches more than one rule in the file, the longest matching rule (the one with the most levels) will be used.
    An exclamation mark (!) at the start of a rule marks an exception to a previous wildcard rule. An exception rule takes priority over any other matching rule.
    The flag is a integer, which indicates WCS how to process a host name which matches to a rule.
    The threshold is a integer, which indicates Pattern Process how to process a host name which matches to a rule. 

Publix Suffix Field:
rule\tflag\tthreshold

usage:  python public_suffix_generator.py -c public_suffix_generator.conf

'''
import conf_util
import aws_s3_util
import gzip
import urlparse
import httplib
import sys
import os
import logging
import time
import hashlib
import urllib
import urllib2
import contextlib
from optparse import OptionParser

#public_suffix_provider = 'https://publicsuffix.org/list/public_suffix_list.dat'
valid_scheme = ['http', 'https']

PUBLIC_SUFFIX_PTN = 'public_suffix.txt'
VERSION_TIME_FORMAT_MIN = '%Y%m%d%H%M'
VERSION_TIME_FORMAT_HOUR = '%Y%m%d%H'
NS_RETRY = 2

class PublicSuffixError(Exception): pass
class PublicSuffixEnvError(Exception): pass
class PublicSuffixDownloadError(PublicSuffixError): pass
class PublicSuffixNormalizeError(PublicSuffixError): pass
class PublicSuffixS3CopyError(PublicSuffixError): pass

def timestamp2str(timestamp, fmt):
    t_struct = time.gmtime(timestamp)
    return time.strftime(fmt, t_struct) 

def get_format_time(org_time, org_fmt, new_fmt):
    return time.strftime(new_fmt, time.strptime(org_time, org_fmt))

class public_suffix_generator(object):
    def __init__(self, config_file):
        self.config_file = config_file
        self.config = self.load_config()
        self.overwrite_config_file_settings()
        self.validate_config()
        self.prepare_env()
        self.logger = self.get_logger()
        self.s3_client =aws_s3_util.S3Handler(proxy= self.config['proxy'], proxy_port= self.config['proxy_port'], connect_timeout = self.config['aws_s3_connect_timeout'], read_timeout = self.config['aws_s3_read_timeout'])

    def load_config(self):
        return conf_util.load_config(self.config_file, ['proxy', 'proxy_port','public_suffix_provider', 'log_level', 'logger_name', 'aws_s3_prefix', 'aws_s3_bucket', 'aws_s3_connect_timeout', 'aws_s3_read_timeout'])


    def set_env_variable(self, var_name):
        if var_name in os.environ and os.environ[var_name] :
            self.config[var_name] = os.environ[var_name]

    def overwrite_config_file_settings(self):
        self.set_env_variable('proxy')
        self.set_env_variable('proxy_port')
        self.set_env_variable('public_suffix_provider')
        self.set_env_variable('log_level')
        self.set_env_variable('logger_name')
        self.set_env_variable('aws_s3_bucket')
        self.set_env_variable('aws_s3_prefix')
        self.set_env_variable('aws_s3_connect_timeout')
        self.set_env_variable('aws_s3_read_timeout')

    def __get_logger(self, logger_name, log_level):
        log_format = '%(name)s[%(asctime)s]-[%(process)s]-[%(levelname)s]: %(message)s'
        log_handler = logging.StreamHandler(sys.stdout)
        # formatter
        log_formatter = logging.Formatter(log_format)
        log_handler.setFormatter(log_formatter)
        # level
        level_dict = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO,
                      'WARN': logging.WARN, 'WARNING': logging.WARNING,
                      'ERROR': logging.ERROR, 'CRITICAL': logging.CRITICAL}
        try:
            log_level = level_dict[log_level.upper()]
        except KeyError:
            log_level = logging.INFO
        # logger
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)
        logger.addHandler(log_handler)
        return logger

    def get_logger(self):
        return self.__get_logger(self.config['logger_name'], self.config['log_level'])

    def prepare_env(self):
        #if not os.path.exists(self.config['public_suffix_generator_root']):
        #    os.makedirs(self.config['public_suffix_generator_root'])
        # create log dir
        current_dir = os.path.abspath('./')
        self.root = current_dir
        self.ptn_dir = os.path.join(self.root, 'ptn')
        self.raw_dir = os.path.join(self.root, 'raw')# the directory to keep raw public suffix
        if not os.path.exists(self.ptn_dir):
            raise PublicSuffixEnvError('Pattern directory %s not exists' %self.ptn_dir)
        if not os.path.exists(self.raw_dir):
            raise PublicSuffixEnvError('Raw download directory %s not exists' %self.raw_dir)
        self.customer_public_suffix_path = os.path.join(self.root, 'custom/customer_public_suffix.txt')
        if not os.path.exists(self.customer_public_suffix_path):
            raise PublicSuffixEnvError('customer public suffix file %s not exists' %self.customer_public_suffix_path)
        self.raw_download_public_suffix_path = os.path.join(self.raw_dir, 'download_public_suffix.txt')
        self.public_suffix_ptn_path = os.path.join(self.ptn_dir, '%s.gz' %PUBLIC_SUFFIX_PTN)
        self.public_suffix_ptn_old = os.path.join(self.ptn_dir, '%s.gz.old' %PUBLIC_SUFFIX_PTN)

    def validate_config(self):
        #conf_util.config_validate_str('proxy', self.config['proxy'])
        #conf_util.config_validate_int('proxy_port', self.config['proxy_port'], 0, 65535)
        conf_util.config_validate_str('public_suffix_provider', self.config['public_suffix_provider'])
        #conf_util.config_validate_str('public_suffix_generator_root', self.config['public_suffix_generator_root'])
        conf_util.config_validate_str('log_level', self.config['log_level'])
        conf_util.config_validate_str('logger_name', self.config['logger_name'])
        conf_util.config_validate_str('aws_s3_prefix', self.config['aws_s3_prefix'])
        conf_util.config_validate_str('aws_s3_bucket', self.config['aws_s3_bucket'])
        conf_util.config_validate_int('aws_s3_connect_timeout', self.config['aws_s3_connect_timeout'], 5, 300)
        conf_util.config_validate_int('aws_s3_read_timeout', self.config['aws_s3_read_timeout'], 5, 300)

    def http_get_public_suffix_data(self, public_suffix_provider):
        data = None
        try:
            if self.config['proxy'] and self.config['proxy_port']:
                proxy_url = "%s:%d" %(self.config['proxy'], self.config['proxy_port'])
                proxies = {'http': proxy_url, 'https': proxy_url}
                proxy_handler = urllib2.ProxyHandler(proxies)
                url_opener = urllib2.build_opener(proxy_handler)
            else :
                url_opener = urllib2.build_opener()
            with contextlib.closing(url_opener.open(public_suffix_provider)) as f:
                data = f.read()
        except Exception, e:
            raise PublicSuffixDownloadError(e)
        if not data:
            raise PublicSuffixDownloadError("Fail to get public suffix from %s. Content length is 0" %(public_suffix_provider))
        return data

    def write_download_public_suffix(self, public_suffix_content):
        f = open(self.raw_download_public_suffix_path, 'w')
        f.write("%s" %public_suffix_content)
        f.close()

    def read_customized_public_suffix_data(self):
        if os.path.exists(self.customer_public_suffix_path):
            f = open(self.customer_public_suffix_path, 'r')
            content = f.read()
            f.close()
            return content
        else:
            return ''

    def puny_code_convert(self, rule):
        # convert punycode
        returl = rule
        punyurl = rule
        try:
            punyurl = unicode(returl, 'utf-8').encode('idna')
            if(returl == punyurl) :
                msg = "Success, doesn't contain any multi-byte in Domain"
            else:
                msg = "Success, url has converted to puny code"
            self.logger.debug("%s[%s]" % (msg, punyurl))
        except Exception, e:
            raise
        return punyurl

    def generate_public_suffix_ptn(self, public_suffix_content):
        lines = public_suffix_content.split('\n')
        fout = gzip.open(self.public_suffix_ptn_path, 'wb')
        for line in lines:
            if len(line) == 0:
                fout.write("%s\n" % line)
                continue
            if line.startswith("//") == True:
                try:
                    line = line.encode("ascii")
                    fout.write("%s\n" % line)
                except Exception, e:
                    try:# if 'ascii' encode fail, try percent encode
                        line = urllib.quote(line)
                        fout.write("%s\n" % line)
                    except Exception, e:
                        fout.write("\n")
                continue
            try:
                prefix = ""
                rule = line
                if line.startswith("*."):
                    prefix = line[0:2]
                    rule = line[2:]
                elif line.startswith("!"):
                    prefix = line[0:1]
                    rule = line[1:]
                rule_ascii = prefix  + self.puny_code_convert(rule)
            except Exception, e:
                    raise
            if rule_ascii is None:
                raise Exception("can't normalize rule %s" % (line))
            fout.write("%s\t0\t-1\n" % (rule_ascii))
        fout.close()

    def is_public_suffix_ptn_checksum_identical(self, old, new):
        if not os.path.exists(old):
            # not found old public suffix table
            # view as not identical and should generate new public suffix table to S3
            return False
        m1 = hashlib.md5()
        m2 = hashlib.md5()
        old_f = gzip.open(old, 'rb')
        old_content = old_f.read()
        with open('old_f', 'w') as f:
            f.write(old_content)
        old_f.close()
        m1.update(old_content)
        old_md5 = m1.hexdigest()
        new_f = gzip.open(new, 'rb')
        new_content = new_f.read()
        with open('new_f', 'w') as f:
            f.write(new_content)
        m2.update(new_content)
        new_md5 = m2.hexdigest()
        if old_md5 == new_md5:
            return True
        else:
            return False

    def get_the_latest_public_suffix_ptn(self):
        try:
            content_filename_list = self.s3_client.list_bucket_content(self.config['aws_s3_bucket'], prefix = self.config['aws_s3_prefix'])
            if not content_filename_list:
                self.logger.info('no reference public suffix pattern in S3')
                self.logger.info('not able to get the latest public suffix pattern')
                return
            content_filename_list.sort()
            current_latest_key_in_s3 = content_filename_list[-1]
            self.s3_client.cp_s3_file_to_local(self.config['aws_s3_bucket'], self.public_suffix_ptn_old, current_latest_key_in_s3)
        except Exception, e:
            raise PublicSuffixS3CopyError(e)
        return

    def save_pattern_to_s3(self, dump_ver):
        try:
            remote_filename = "%s.%s.gz" %(PUBLIC_SUFFIX_PTN, dump_ver)
            remote_path = os.path.join( self.config['aws_s3_prefix'], remote_filename)
            s3_filename_list = self.s3_client.list_bucket_content( self.config['aws_s3_bucket'], prefix = self.config['aws_s3_prefix'])
            # check if the specified pattern already in S3 
            # if already in S3, return directly
            if remote_path in s3_filename_list:
                self.logger.info('public suffix version %s already in AWS S3 storage' %dump_ver)
                return
            # if not in S3, copy to S3
            self.s3_client.cp_local_file_to_s3( self.config['aws_s3_bucket'], self.public_suffix_ptn_path, remote_path, customer_sse_key= None)
            s3_filename_list.append(remote_path)
        except Exception, e:
            raise PublicSuffixS3CopyError(e)

    def run(self):
        returncode = 0
        try:
            # 1. download the latest public suffix table
            self.logger.info('download public suffix from [%s]' % self.config['public_suffix_provider'])
            public_suffix_content = self.http_get_public_suffix_data( self.config['public_suffix_provider'])
            # 2. get the latest public suffix pattern from S3 bucket
            self.logger.info('get the latest public suffix pattern from S3')
            #self.get_the_latest_public_suffix_ptn()
            # 3. write raw public suffix
            self.logger.info('write raw public suffix data')
            self.write_download_public_suffix(public_suffix_content)
            # 4. read local customized public suffix table
            self.logger.info('read customer\'s public suffix data')
            customer_public_suffix_content = self.read_customized_public_suffix_data()
            # 5. merge download public suffix table with customized table
            self.logger.info('merge download and customer\'s public suffix data')
            merged_public_suffix_content = public_suffix_content + '\n' \
                                           + '// ===BEGIN WCS TESTKIT DOMAINS' +'\n' \
                                           + customer_public_suffix_content + '\n'  \
                                           + '// ===END WCS TESTKIT DOMAINS\n'
            # 6. generate public suffix pattern
            self.logger.info('generate public suffix pattern')
            self.generate_public_suffix_ptn(merged_public_suffix_content)
            # 7. compare checksum of new and the latest public suffix in S3
            #is_identical = self.is_public_suffix_ptn_checksum_identical(self.public_suffix_ptn_old, self.public_suffix_ptn_path)
            # 8. Copy to S3
            '''
            if is_identical:
                self.logger.info('puglic suffix pattern is not updated')
                self.logger.info('do not have to generate new public suffix pattern')
            else: 
                self.logger.info('copy public suffix pattern to S3')
                # convert current timestamp to '%Y%m%d%H' format
                dump_ver = timestamp2str(int(time.time()), VERSION_TIME_FORMAT_HOUR)
                # extend time to '%Y%m%d%H%M' format
                dump_ver = get_format_time(dump_ver, VERSION_TIME_FORMAT_HOUR, VERSION_TIME_FORMAT_MIN)
                self.save_pattern_to_s3(dump_ver)
            '''
        except Exception, e:
            self.logger.error("fail to generate public suffix pattern. Error: %s" %e)
            returncode = -1
        return returncode

def parse_args():
    parser = OptionParser(option_class=conf_util.ConfigOption)
    parser.add_option('-c', '--config', help = 'path of config file', dest = 'config', action = 'store', type = 'string')
    (opts, args) = parser.parse_args()
    return (opts.config)


def main(argv):
    if len(argv) !=3:
        print >> sys.stderr, 'Usage: %s -c [ConfigFileName]' %(argv[0])
        print >> sys.stderr, 'Example: %s -c ./conf/new_domain_redis_agent.conf' %(argv[0])
        return -1
    (config_file) = parse_args()
    if not config_file:
        print >> sys.stderr, 'Fail to parse --config argument.'
        return -1
    psg = public_suffix_generator(config_file)
    return psg.run()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
