from web3 import Web3

# Constants
ETH_PRECISION = int(10**18)

# Instantiate a web3 connection
def setup_web3_connection():
    w3 = Web3(Web3.IPCProvider(request_kwargs={'timeout': 300}))
    assert (w3.isConnected() and not w3.eth.syncing), "No web3 connection or node not synced."
    return w3


# Get keccak hash of a string
def to_keccak(sig):
    return str(Web3.keccak(text=sig).hex())


# Get the sum of some quantity from a given filter
def get_sum_over_filter(filt, parser):
    amount = 0
    for event in filt.get_new_entries():
        amount += parser(event)
    return amount
