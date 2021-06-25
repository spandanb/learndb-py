import inspect

# disabling makes tests a lot faster
DEBUG = False


def get_caller_info() -> str:
    """
    return caller name and lineno
    :return:
    """
    info = ""
    caller = None
    try:
        stack = inspect.stack()
        # note stack 0,1 are `get_caller` and `debug` skip those and
        # get the caller of `debug`
        caller = stack[2]
        info = f"{caller.function}({caller.lineno})"
        return info
    finally:
        del caller
        del stack


def debug(string: str):
    """
    :param args:
    :param kw:
    :return:
    """

    if DEBUG:
        metadata = get_caller_info()
        print(metadata + ": " + string)