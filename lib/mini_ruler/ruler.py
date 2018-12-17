# -*- coding:utf-8 -*-

import re
from calc_tokens import calc
from lexer import RuleLexer
import basic_action


class RulerEnv:

    def __init__(self):
        self.variables_stack = [{}]
        self.stack_top = self.variables_stack[-1]

    def push(self):
        self.variables_stack.append({})
        self.stack_top = self.variables_stack[-1]

    def pop(self):
        if len(self.variables_stack) == 1:
            raise Exception('variables_stack is 1, can not pop variable stack, ')
        self.variables_stack.pop()
        self.stack_top = self.variables_stack[-1]

    def foreach_get_var(self, var_str):
        for level in self.variables_stack[::-1]:
            var = level.get(var_str, None)
            if var is not None:
                break
        else:
            return None
            # raise Exception("name '%s' is not defind" % var_str)
        return var

    def set_var(self, var_name, value):
        if value is not None:
            self.stack_top[var_name] = value

    def set_global_var(self, var_name, value):
        self.variables_stack[0][var_name] = value

    def clear_global_var(self, var_name):
        del self.variables_stack[0][var_name]

    def get_global_var(self, var_name):
        return self.variables_stack[0].get(var_name, None)

    def get_var(self, var_str):
        var_str_list = var_str.split('.')
        var_name, var_list = var_str_list[0], var_str_list[1:]

        var = self.foreach_get_var(var_name)
        if var is None:
            return None

        result, s = var, var_str_list[0]
        for k in var_list:
            if type(result) != dict:
                return None
            v = result.get(k, None)
            if v is None:
                return None
            result = v
            s += '.%s' % k

        return result


def parse_rule(rule_str):
    rule_lines = rule_str.split("\n")
    current_rule = None
    result = {}

    for i, line in enumerate(rule_lines):
        line = line.replace('\n', '')
        line_no = i + 1

        regMatch = re.match(r'^(?P<rule_name>[a-zA-Z_][a-zA-Z0-9_]*):\s*$', line)
        if regMatch:  # is rule_name line
            rule_name = regMatch.groupdict()['rule_name']
            # print "start rule %s: " % rule_name
            if rule_name in result:
                raise Exception("Duplicate rule '%s' at line %s" % (rule_name, line_no))
            result[rule_name] = []
            current_rule = rule_name
        elif re.match(r'^(\t|    )IF', line):  # is rule line
            # print "rule: ", line
            if not current_rule:
                raise Exception("No rule_name found befroe line %s" % line_no)
            line = ' '.join(line.split()).lstrip().rstrip()
            result[current_rule].append(line)
        elif re.match(r'^\s*#', line):  # is comment line
            # print "comment: ", line
            continue
        elif re.match(r'^\s*$', line):  # is empty line
            # print "empty: ", line
            continue
        else:
            raise Exception("Error rule at line %s :\n`%s`" % (line_no, line))
    return result


def parse_rule_file(path):
    with open(path, 'r') as f:
        rule_str = f.read()
        return parse_rule(rule_str)


def split_rule(rule):
    pattern = r"^IF (?P<cond>[\S ]+) THEN (?P<action>[\S ]+)"
    reg = re.compile(pattern)
    match = reg.match(rule)
    if not match:
        return None
    match = match.groupdict()
    return match['cond'], match['action']


class RulerError(Exception):
    def __init__(self, err):
        Exception.__init__(self, err)


class RulerNoMatch(Exception):
    pass


class RulerGoto(Exception):
    def __init__(self, rule_name):
        Exception.__init__(self, rule_name)

def goto_rule(rule_name):
    raise RulerGoto(rule_name)


class Ruler:

    def __init__(self):
        self.rule_set_list = {}
        self.env = RulerEnv()
        self.lexer = RuleLexer()
        self.init_builtin_action()

    def init_builtin_action(self):
        self.register_action('exist', basic_action.exist)
        self.register_action('re_match', basic_action.re_match)
        self.register_action('num_in_range', basic_action.num_in_range)
        self.register_action('ip_in_net', basic_action.ip_in_net)
        self.register_action('goto', goto_rule)

    def build_rule(self, rule):
        cond_str, then_str = split_rule(rule)
        cond_tokens = self.lexer.parse_tokens(cond_str)
        then_tokens = self.lexer.parse_tokens(then_str)
        return rule, cond_tokens, then_tokens

    def register_rule_set(self, name, rule_list):
        if name in self.rule_set_list:
            raise RulerError("can not resiger rule set '%s' reason: already exist")
        self.rule_set_list[name] = [self.build_rule(rule) for rule in rule_list]

    def clear_rule_set(self, name):
        del self.rule_set_list[name]

    def register_action(self, name, action):
        if self.env.get_global_var(name) is not None:
            raise RulerError("can not resiger action '%s' reason: already exist" % name)
        self.env.set_global_var(name, action)

    def clear_action(self, name):
        self.env.clear_global_var(name)

    def foreach_rule_set(self, name):
        """
        遍历规则集，获得规则集的结果。
        每条规则由 Condition 和 Action 组成， Condition 若匹配中则执行 Action, 并返回Action的返回值作为Rule的返回值
        """
        for rule_obj in self.rule_set_list[name]:
            rule, cond_tokens, then_tokens = rule_obj[0], rule_obj[1], rule_obj[2]
            result = calc(self.env, cond_tokens)
            if result:
                try:
                    result = (calc(self.env, then_tokens), rule)
                except RulerGoto as e:
                    rule_name = e[0]
                    try:
                        result = self.foreach_rule_set(rule_name)
                    except RulerNoMatch:
                        continue
                return result

        raise RulerNoMatch

    def entry(self, name, p):
        if name not in self.rule_set_list:
            raise RulerError("Rule set '%s' does not exist" % name)

        if type(p) != dict:
            raise RulerError("Ruler.entry must input a dict, but not %s" % type(p))

        self.env.push()
        for k, v in p.items():
            self.env.set_var(k, v)
        result = self.foreach_rule_set(name)
        self.env.pop()

        return result
