import pytest

def calculate(num1, op, num2):
    if op == "+":
        return num1 + num2
    elif op == "-":
        return num1 - num2
    elif op == "*":
        return num1 * num2
    elif op == "/":
        return num1 / num2

def test_plus():
    '''Test plus'''
    result = calculate(1, '+', 1)
    assert result == 2

def test_multipy():
    '''Test multipy'''
    result = calculate(1, '*', 1)
    assert result == 1

def test_subtract():
    '''Test subtract'''
    result = calculate(1, '-', 1)
    assert result == 0

def test_devision():
    '''Test devision'''
    result = calculate(1, '/', 1)
    assert result == 1
