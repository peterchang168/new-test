#!/usr/bin/python2.6


def return_a():
    return 'a'

def return_b():
    return 'b'

def return_c():
    return 'c'

def return_d():
    return 'd'

def main(argv):
    return_a()
    return_b()
    return_c()
    return_d()

if __name__ == '__main__':
    sys.exit(main(sys.argv))
