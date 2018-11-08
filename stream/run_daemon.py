# -*- coding:utf-8 -*-
import os
import re
import time
import json
import signal
import traceback

from daemon import runner

from stream import StreamController
from log import init_logger


def remove_json_commets(conf_str):
    lines = conf_str.split('\n')
    new = ''
    for line in lines:
        line = line.strip(' ')
        if not re.search(r'^\s*//', line):
            new += line
    new = new.replace(',}', '}')
    new = new.replace(', }', '}')
    new = new.replace(',]', ']')
    new = new.replace(', ]', ']')
    new = new.replace('{', '{\n')
    return new


class Server(object):

    def __init__(self, name, run_config):
        self.stdin_path = run_config.get("STDIN", "/dev/null")
        self.stdout_path = run_config.get("STDOUT", "/dev/null")
        self.stderr_path = run_config.get("STDERR", "/dev/null")
        self.pidfile_path = run_config.get("PID_FILE", "/var/run/%s.pid" % name)
        self.pidfile_timeout = run_config.get("PID_FILE_TIMEOUT", 3)

        self.logfile = run_config['LOG_FILE']
        self.loglevel = run_config.get('LOG_LEVEL', 'info')
        self.consolelog = run_config.get('CONSOLE_LOG', False)

        conf = self.read_conf(run_config['CONF_FILE_PATH'])
        if not conf:
            raise Exception("can not get stream config!!!!")

        poll_time = run_config.get('STREAM_CONTROLLER_POLL_TIME', 1)

        self.stream_ctrl = StreamController(conf, poll_time=poll_time)

    def read_conf(self, conf_path):
        f = open(conf_path, 'r')
        conf = f.read()
        f.close()
        conf = remove_json_commets(conf)
        return json.loads(conf)

    def init_signal_handler(self, logger=None):
        def sig_handler(signum, frame):
            if logger:
                logger.warning("Recv signal %s %s" % (signum, frame))
            self.stream_ctrl.stop()

        signal.signal(signal.SIGHUP, sig_handler)
        signal.signal(signal.SIGINT, sig_handler)
        signal.signal(signal.SIGQUIT, sig_handler)
        signal.signal(signal.SIGTERM, sig_handler)

    def run(self):
        logger = init_logger(self.loglevel, self.logfile, self.consolelog)

        self.init_signal_handler(logger)

        self.stream_ctrl.start()


def get_pid_from_file(pid_file):
    if not os.path.exists(pid_file):
        return None
    f = open(pid_file, 'r')
    pid = f.read().replace('\n', '')
    f.close()
    return pid


def start(server, pid_file):
    pid = get_pid_from_file(pid_file)
    if not pid:
        daemon_runner = runner.DaemonRunner(server)
        daemon_runner.do_action('start')
        pid = get_pid_from_file(pid_file)
        print("Process %s start success!" % pid)
    else:
        print("Program is already run as %s" % pid)


def stop(server, pid_file):
    pid = get_pid_from_file(pid_file)
    if not pid:
        print("Program not in run")
    else:
        daemon_runner = runner.DaemonRunner(server)
        daemon_runner.do_action('stop')
        while True:
            if not os.path.exists(pid_file):
                break
            pid = get_pid_from_file(pid_file)
            print("Waiting for Process %s stop" % pid)
            time.sleep(1)
        print("Process %s stop success!" % pid)


def run_daemon(name, run_config, op='start', daemon=True):
    server = Server(name, run_config)
    if daemon == False:
        server.run()
    else:
        pid_file = run_config['PID_FILE']
        if op == 'start':
            start(server, pid_file)
        elif op == 'restart':
            stop(server, pid_file)
            start(server, pid_file)
        elif op == 'stop':
            stop(server, pid_file)
        else:
            raise Exception("Unkonw op %s" % op)
