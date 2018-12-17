# -*- coding:utf-8 -*-


def calc_not_operate(x):
    if type(x) == bool:
        return not x
    if type(x) in [int, float]:
        return not x
    if x is None:
        return True
    return False


calc_tbl = {
    '!': lambda x, y=None: calc_not_operate(x),
    '~=': lambda x, y: x in y,

    '&&': lambda x, y: x and y,
    '||': lambda x, y: x or y,

    '>': lambda x, y: x > y,
    '>=': lambda x, y: x >= y,
    '<': lambda x, y: x < y,
    '<=': lambda x, y: x <= y,
    '==': lambda x, y: x == y,
    '!=': lambda x, y: x != y,

    '*': lambda x, y: x * y,
    '/': lambda x, y: x / y,
    '+': lambda x, y: x + y,
    '-': lambda x, y: x - y,
}

def baisc_calc(op, x, y=None):
    result = calc_tbl[op](x, y)
    return result

def get_token_value(env, tok):
    if tok[0] == 'ID':
        value = env.get_var(tok[1])
    elif tok[0] == 'CALL':
        call, args = tok[1][0], tok[1][1]
        call = env.get_var(call)
        new_args = [ get_token_value(env, arg) for arg in args ]
        value = call(*new_args)
    else:
        value = tok[1]
    return value


def create_token(x):
    if type(x) == unicode: x = str(x)
    if type(x) == int: return ('INTEGER', x)
    if type(x) == float: return ('FLOAT', x)
    if type(x) == str: return ('STRING', x)
    if type(x) == bool:  return ('BOOL', x)
    if type(x) == type(None): return ('NULL', x)
    if type(x) == type(dict): return ('DICT', x)
    if type(x) == type(list): return ('LIST', x)
    if type(x) == type(tuple): return ('TUPLE', x)

    # 目前还不支持 None, list, dict, tple 等类型, 做特殊处理
    # # 当获取到None时， 为False
    # if type(x) == type(None):  return ('BOOL', False)
    # # 为了只有一个id变量的时候，获取到这些的空值导致判断结果为False
    # if type(x) in [dict, list, tuple]:  return ('BOOL', True)
    raise Exception('Unknow token type %s %s' % (type(x), x))


def token_calc(env, op, xtok, ytok=None):
    op = op[1]
    x = get_token_value(env, xtok)
    # print("Get %s value=%s" % (xtok, x))
    y = get_token_value(env, ytok) if ytok else None
    # print("Get %s value=%s" % (ytok, y))

    result = baisc_calc(op, x, y)
    result = create_token(result)
    return result


class OperatorError(Exception):
    def __init__(self, err):
        Exception.__init__(self, err)


class Operator():
    op_tbl = [
        # 优先级低
        ['='],
        ['||'],
        ['&&'],
        ['==', '!=', '~='],
        ['>', '>=', '<', '<='],
        ['+', '-'],
        ['*', '/'],
        ['!'],
        ['.'],
        ['call', 'item'],
        # 优先级高
    ]

    op_level = {}
    for i, one_level in enumerate(op_tbl):
        for op in one_level:
            op_level[op] = i

    @classmethod
    def level(cls, op):
        if not op in cls.op_level:
            raise OperatorError("operator `%s` does not support" % op)
        return cls.op_level[op]


class CalcStack:

    def __init__(self):
        self.stack = [([], [])]
        self.operands = self.stack[-1][0]
        self.operators = self.stack[-1][1]

    def operators_stack_top(self):
        if len(self.operators) > 0:
            return self.operators[-1]
        return None

    def push_operator(self, tok):
        self.operators.append(tok)
        # print("\npush operator %s" % str(tok))
        # print("optr: " + str(self.operators))
        # print("oprd: " + str(self.operands))

    def pop_operator(self):
        tok = self.operators.pop()
        # print("\nPop operator %s" % str(tok))
        # print("optr: " + str(self.operators))
        # print("oprd: " + str(self.operands))
        return tok

    def operands_stack_top(self):
        if len(self.operands) > 0:
            return self.operands[-1]
        return None

    def push_operand(self, tok):
        self.operands.append(tok)
        # print("\npush operand %s" % str(tok))
        # print("optr: " + str(self.operators))
        # print("oprd: " + str(self.operands))

    def pop_operand(self):
        tok = self.operands.pop()
        # print("\nPop operand %s" % str(tok))
        # print("optr: " + str(self.operators))
        # print("oprd: " + str(self.operands))
        return tok

    def push(self):
        self.stack.append(([], []))
        self.operands = self.stack[-1][0]
        self.operators = self.stack[-1][1]

    def pop(self):
        operands, operators = self.stack.pop()
        self.operands = self.stack[-1][0]
        self.operators = self.stack[-1][1]
        return operands, operators

def calc(env, tokens):

    result = []

    stack = CalcStack()

    def calc_stack_top():
        # print "calc_stack_top"
        optr = stack.pop_operator()
        if optr[0] in ['NOT_OPERATOR']: # 单目运算符
            oprd2, oprd1 = None, stack.pop_operand()
        else: # 双目运算符
            oprd2, oprd1 = stack.pop_operand(), stack.pop_operand()
        result = token_calc(env, optr, oprd1, oprd2)
        # print("Calc %s %s %s = %s" % (oprd1, optr, oprd2, result))
        stack.push_operand(result)

    def flush_out_stack():
        while stack.operators_stack_top():
            calc_stack_top()
        if stack.operators_stack_top():
            raise Exception('Stack is not balance')
        result.append(stack.operands[0])

    tok_iter = iter(tokens)

    tok = tok_iter.next()
    while True:
        try:
            # print tok

            if tok[0] in ['INTEGER', 'FLOAT', 'STRING', 'BOOL', 'NULL']:
                stack.push_operand(tok)
                tok = tok_iter.next()
            elif tok[0] in ['ID', 'CALL']:
                stack.push_operand(tok)
                # stack.push_operand(create_token(get_token_value(env, tok)))
                tok = tok_iter.next()
            elif tok[0] in ['LOGICAL_OPERATOR', 'RELATIONAL_OPERATOR', 'ARITHMEITC_OPERATOR', 'NOT_OPERATOR']:
                stack_top = stack.operators_stack_top()
                if stack_top:
                    if Operator.level(stack.operators[-1][1]) < Operator.level(tok[1]):
                        stack.push_operator(tok)
                        tok = tok_iter.next()
                    else:
                        calc_stack_top()
                else:
                    stack.push_operator(tok)
                    tok = tok_iter.next()
            elif tok[0] == 'LPAREN':
                stack.push()
                tok = tok_iter.next()
            elif tok[0] == 'RPAREN':
                flush_out_stack()
                operands, _ = stack.pop()
                stack.operands.append(operands)
                tok = tok_iter.next()
            else:
                raise Exception('Unknow tok %s' % str(tok))
        except(StopIteration):
            flush_out_stack()
            break

    # print stack.operands_stack_top()
    result = get_token_value(env, stack.operands_stack_top())
    # print result
    return result
