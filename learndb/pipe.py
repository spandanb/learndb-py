from collections import deque


class Pipe:
    """
    Used to read results from select expr
    """

    def __init__(self):
        self.store = deque()

    def write(self, msg):
        self.store.append(msg)

    def has_msgs(self) -> bool:
        return len(self.store) > 0

    def read(self):
        """
        Read message and remove from the pipe
        """
        return self.store.popleft()

    def reset(self):
        self.store = deque()
