from collections import deque

class BufferPool:
    def __init__(self, size):
        self.size = size
        self.buffer_blocks = deque(maxlen=size)  # Queue to store blocks

    def add_block(self, page):
        if len(self.buffer_blocks) == self.size:
            self.evict_block()
        self.buffer_blocks.append(page)

    def evict_block(self):
        if self.buffer_blocks:
            evicted_page = self.buffer_blocks.popleft()
            print("Evicted page:", evicted_page)
            return evicted_page
        else:
            print("Buffer pool is empty.")
            return None
    
    def has_capacity(self,)