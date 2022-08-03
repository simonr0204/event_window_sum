from utils import setup_web3_connection, to_keccak, get_sum_over_filter, ETH_PRECISION
import time


def main(contract_address, event_sig, event_parser, sum_parser, window):

    print(f"Sum of {event_sig} events on {contract_address[:10]}..., window = {window}")
    print("Listening...")

    # Instantiate a web3 connection
    w3 = setup_web3_connection()

    # Map to hold sums per block timestamp
    sums = dict()

    # Filter to listen for events
    filt = w3.eth.filter(
        {
            "fromBlock": 'latest',
            "toBlock": 'latest',
            "address": contract_address,
            "topics": [to_keccak(event_sig)],
        }
    )

    # Only calculate new events when a new block is received
    old_block = 0
    while True:
        if (new_block := w3.eth.blockNumber) != old_block:
            block_timestamp = w3.eth.get_block("latest")["timestamp"]
            if (sum_in_current_block := get_sum_over_filter(filt, event_parser)) != 0:
                sums[block_timestamp] = sum_in_current_block
            old_block = new_block
            
            # Discard sums that happened before the current window
            sums = {k: v for k,v in sums.items() if k > block_timestamp - window}

            window_sum = sum(sums.values())
            sum_parser(window_sum)  

        time.sleep(1)

    

if __name__ == "__main__":

    # Configure script with the following:

    # The address and event signature to listen to
    CONTRACT_ADDRESS = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
    EVENT_SIG = "Transfer(address,address,uint256)"


    # A parser for extracting values from the specified event
    # For this example, DAI transfer is `Transfer(address indexed src, address indexed dst, uint wad)`
    # So `amount` is in the data
    def parse_dai_transfer(event):
        return int(event['data'], 16)


    # A parser for the window sum
    # e.g.
    # if window_sum > threshold:
    #   do_something()
    # For this example, just print the sum if > 0
    def parse_dai_sum(window_sum):
        if window_sum > 0:
            print(f"{window_sum / ETH_PRECISION :.0f}")


    # The length of the rolling window to sum event values over
    window = 60


    main(
        contract_address=CONTRACT_ADDRESS,
        event_sig = EVENT_SIG,
        event_parser=parse_dai_transfer,
        sum_parser = parse_dai_sum,
        window=60,
        )
