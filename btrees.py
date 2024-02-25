try:
    from BTrees.OOBTree import OOBTree
    print("BTrees is installed and available.")
except ImportError:
    print("BTrees is not installed or there's an issue with its installation.")
