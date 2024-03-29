import threading
from lstore.table import Table, Record
from lstore.index import Index

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        pass

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        self.thread = threading.Thread(target = self.__run, daemon=1)
        self.thread.start()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        self.thread.join()

    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            run_result = transaction.run()
            self.stats.append(run_result)
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

