import collections

KnownReplyError = collections.namedtuple(
    "KnownReplyError",
    ["reply", "args"],
)


def parse_cluster_response_error(err):
    if not err or not err.args or not err.args[0]:
        return None
    data = err.args[0].strip()
    if data.startswith("MOVED"):
        return KnownReplyError(reply="MOVED",
                               args=parse_new_host_response_error(data))
    elif data.startswith("CLUSTERDOWN"):
        return KnownReplyError(reply="CLUSTERDOWN", args=None)
    elif data.startswith("ASK"):
        return KnownReplyError(reply="ASK",
                               args=parse_new_host_response_error(data))
    elif data.startswith("TRYAGAIN"):
        return KnownReplyError(reply="TRYAGAIN", args=None)
    return None


def parse_new_host_response_error(data):
    try:
        host, port = data.split()[-1].split(':')
        return host, int(port)
    except IndexError:
        return None
