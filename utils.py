import inspect

DEBUG = True


def get_caller_info() -> str:
    """
    return caller name and lineno
    :return:
    """
    info = ""
    try:
        stack = inspect.stack()
        # note stack 0,1 are `get_caller` and `debug` skip those and
        # get the caller of `debug`
        caller = stack[2]
        info = f"{caller.function}({caller.lineno})"
        return info
    finally:
        del caller


def debug(string: str):
    """
    :param args:
    :param kw:
    :return:
    """

    if DEBUG:
        metadata = get_caller_info()
        print(metadata + ": " + string)