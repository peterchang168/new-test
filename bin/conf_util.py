#!/bin/env python

import os
import socket
from itertools import chain
from optparse import OptionParser
from optparse import Option


class ConfigKeyError(Exception): pass
class ConfigLostKeyError(Exception): pass
class ConfigParsingError(Exception): pass
class ConfigFormatError(Exception): pass

class ConfigOption(Option):

    ACTIONS = Option.ACTIONS + ("extend",)
    STORE_ACTIONS = Option.STORE_ACTIONS + ("extend",)
    TYPED_ACTIONS = Option.TYPED_ACTIONS + ("extend",)
    ALWAYS_TYPED_ACTIONS = Option.ALWAYS_TYPED_ACTIONS + ("extend",)

    def take_action(self, action, dest, opt, value, values, parser):
        if action == "extend":
            lvalue = value.split(",")
            for token in lvalue:
                token = token.strip()
                if token:
                    values.ensure_value(dest, []).append(token)
        else:
            Option.take_action(
                self, action, dest, opt, value, values, parser)

def read_config_file(config_file):
    config_data = 'config = {}\n'
    config_data += open(config_file, 'r').read()
    return config_data

def parse_config(conf_content):
    config = {}    
    try:
        exec(compile(conf_content, '', "exec"))
    except Exception, e:
        raise ConfigParsingError('Exception occurred while parsing config: %s' \
                % e)
    if not isinstance(config, dict):            
        raise ConfigFormatError('config is not dictionary format ')
    return config 
    
def check_config_keys(config, exp_key_list):
    exp_key_set = set(exp_key_list)
    config_key_set = set(config.keys())
    lost_key_set = exp_key_set - config_key_set
    return list(lost_key_set)
    

def load_config(conf_file_path, essential_key_list = None):
    conf_data = read_config_file(conf_file_path)
    if conf_data :
        config = parse_config(conf_data)
    else :
        raise ConfigFormatError('Config content is empty')
    if essential_key_list is None:
        return config
    lost_keys = check_config_keys(config, essential_key_list)
    if len(lost_keys) > 0:
        raise ConfigLostKeyError('lost config %s' %lost_keys)
    return config


def config_validate_bool(var_name, value):
    if type(value) != bool:
        raise ConfigKeyError('"%s" is not a boolean value'%(var_name))


def config_validate_str(var_name, value):
    if type(value) is not str:
        raise ConfigKeyError('"%s" is not a string.'%(var_name))

    if not value:
        raise ConfigKeyError('"%s" should not empty or None. ' \
                'The number is %s.' % (var_name, value))


def config_validate_int(var_name, value, min, max):
    if type(value) != int:
        raise ConfigKeyError('"%s" is not an integer.'%(var_name))

    if value < min or value > max:
        raise ConfigKeyError('"%s" should not be < %s or > %s. ' \
                'The number is %s.' % (var_name, min, max, value))


def config_validate_float(var_name, value, min, max):
    if type(value) != float:
        raise ConfigKeyError('"%s" is not a float.'%(var_name))

    if value < min or value > max:
        raise ConfigKeyError('"%s" should not be < %s or > %s. ' \
                'The number is %s.' % (var_name, min, max, value))


def config_validate_numeric(var_name, value, min, max):
    if type(value) != int and type(value) != float:
        raise ConfigKeyError('"%s" is not a numeric value'%(var_name))

    if value < min or value > max:
        raise ConfigKeyError('"%s" should not be < %s or > %s, ' \
                'the number is %s' % (var_name, min, max, value))


def config_validate_file(var_name, value):
    if not isinstance(value, basestring):
        raise ConfigKeyError('"%s" is not a string."' % value)

    if not os.path.isfile(value):
        raise ConfigKeyError('"%s" is not a valid filename ' \
                'or does not exist. [%s]' % (var_name, value))


def config_validate_dir(var_name, value):
    if not isinstance(value, basestring):
        raise ConfigKeyError('"%s" is not a string."' % value)

    if not os.path.isdir(value):
        raise ConfigKeyError('"%s" is not a valid directory name ' \
                'or does not exist. [%s]' % (var_name, value))


def config_validate_list(var_name, value):
    if type(value) != list:
        raise ConfigKeyError('"%s" is not a list'%(var_name))

    if len(value) == 0:
        raise ConfigKeyError('"%s" should has at least one element ' \
                'and the content is %s' % (var_name,  value))


def config_validate_hostname(var_name, value):
    try:
        socket.getaddrinfo(value, 0)
    except socket.gaierror, err:
        raise ConfigKeyError('"%s" => "%s" hostname cannot be resolved.' \
                %(var_name, value))

