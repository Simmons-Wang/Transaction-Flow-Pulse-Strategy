from src.exchangeFlow import flowTracker
from src.analib import get_historical_data
import datetime

tracker=flowTracker()
# tracker.get_proxy()
# tracker.save_valid_address()
flows = tracker.get_freq_trans()
price = get_historical_data('ETH/USD',
                            datetime.datetime(2022, 5, 27),
                            datetime.datetime(2022, 6, 9), 60)

