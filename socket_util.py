import socket
import fcntl
import struct
import subprocess
import time

'''
    This module has left the exception handling and params check.
'''

def get_ifnames(cmd=None):

    '''Get all ifnames using shell command `ifconfig -s` and return a ifname list. You can use your own `cmd` to get the ifname list

        params: 
            cmd: shell command. You may leave it as default
        returns:
            (status, list of ifnames)
    '''
    if not cmd:
        cmd = """ifconfig -s | grep -i -v Iface | awk '{print $1}'"""

    sp = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    sout, serr = sp.communicate()

    if not serr:
        #TODO: unicode ifname support
        name = sout.strip()
        names = name.split()
        return(0, names)
    else:
        return (-1, serr)

def get_ip_address(ifname, convert=True):

    '''Get ip address from ifname. 
        
        params:
            ifname: interface/device name
            convert: wheather returns four_bytes_ip or string_ip. Default is True
        returns:
            string_ip (default) or four_bytes_ip
    '''
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ip_int = fcntl.ioctl(s.fileno(),0x8915,struct.pack('256s', ifname[:15]))[20:24]
    if convert:
        return socket.inet_ntoa(ip_int)
    else:
        return list(ip_int)

def is_internet_ip(ip):

    '''Check if is a internet ip and returns true if it is. Only for IPv4

        A: 10.0.0.0~10.255.255.255  mask 10.0.0.0/8
        B: 172.16.0.0~172.31.255.255  mask 172.16.0.0/12
        C: 192.168.0.0~192.168.255.255  mask 192.168.0.0/16
        loopback: 127.0.0.0~127.255.255.255  mask 127.0.0.0/8
    '''


    a_class = convert_to_int("10.0.0.0") >> 24
    b_class_1 = convert_to_int("172.16.0.0") >> 20
    b_class_2 = convert_to_int("172.31.0.0") >> 20
    c_class = convert_to_int("192.168.0.0") >> 16
    local_loop = convert_to_int("127.0.0.0") >> 24

    ret_int_a_or_loop = convert_to_int(ip) >> 24
    ret_int_b = convert_to_int(ip) >> 20
    ret_int_c = convert_to_int(ip) >> 16

    if ret_int_a_or_loop == a_class:
        return "is an a_class ip"
    elif ret_int_a_or_loop == local_loop:
        return "is a loopback ip"
    elif ret_int_b >= b_class_1 and ret_int_b <= b_class_2:
        return "is a b_class ip"
    elif ret_int_c == c_class:
        return "is a c_class ip"
    else:
        return "true"

def time_it(func):

    def wrapper(*args, **kargs):

        start = time.time()
        ret = func(*args, **kargs)
        end = time.time()
        print "===== %s time used =====" % func.__name__
        print end - start
        return ret
    return wrapper

@time_it
def convert(ip):
    
    return sum(int(r) << ((3-i)<<3) for i, r in enumerate(ip.strip().split(r".")))


@time_it
def convert_to_int(ip):

    if isinstance(ip, str):
        return socket.ntohl(struct.unpack("I",socket.inet_aton(str(ip)))[0])
    else:
        return ip


if __name__ == "__main__":

#    ret = get_ifnames()
#    if ret[0] == 0:
#        for ifname in ret[1]:
#            print ifname, get_ip_address(ifname, convert=False)
#    else:
#        print ret[1]

#    print is_internet_ip("192.168.1.1")
#    print is_internet_ip("10.168.1.1")
#    print is_internet_ip("172.16.1.1")
#    print is_internet_ip("172.31.1.1")
#    print is_internet_ip("127.31.1.1")

    print convert("192.168.1.1")
    print convert_to_int("192.168.1.1")
