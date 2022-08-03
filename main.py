from utils import setup_web3_connection, to_keccak, get_sum_over_filter, ETH_PRECISION
import time


def main(contract_address, event_sig, event_parser, aggregate_parser, aggregator, window):

    print(f"Aggregating {event_sig} events on {contract_address[:10]}... over a {window}s window")
    print("Listening...")

    # Instantiate a web3 connection
    w3 = setup_web3_connection()

    # Map to hold sums per block timestamp
    sums = dict()

    # Filter to listen for events
    filt = w3.eth.filter(
        {
            "fromBlock": "latest",
            "toBlock": "latest",
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

            # If dict is not empty, aggregate the sums over the window and parse the result
            if sums:
                window_agg = aggregator(sums.values())
                aggregate_parser(window_agg)  

        time.sleep(1)

    

if __name__ == "__main__":

    # Configure script with the following:

    # Use Dai transfer events as example
    CONTRACT_ADDRESS = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
    EVENT_SIGNATURE = "Transfer(address,address,uint256)"


    # A parser for extracting values from the specified event
    # For this example, DAI transfer is `Transfer(address indexed src, address indexed dst, uint wad)`
    # So `amount` is in the data
    def parse_dai_transfer(event):
        return int(event["data"], 16)


    # A parser for the window aggregate
    # For this example, just print the aggregate if > 0
    def parse_dai_aggregate(window_agg):
        if window_agg > 0:
            print(f"{window_agg / ETH_PRECISION :.0f}")


    # The length of the rolling window to aggregate event values over
    window = 60

    # The aggregator function to use over the window
    # (E.g. sum, mean, custom function, etc.)
    aggregator = sum

    main(
        contract_address = CONTRACT_ADDRESS,
        event_sig = EVENT_SIGNATURE,
        event_parser = parse_dai_transfer,
        aggregate_parser = parse_dai_aggregate,
        aggregator = aggregator,
        window = window,
        )
