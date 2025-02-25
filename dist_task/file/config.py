
USER = "ubuntu"


def is_remote(host):
    return host and host not in ('local', 'localhost', '127.0.0.1')
