import backtrader as bt
from helper_functions import time_it


# Custom data feed class
class AlpacaStockData(bt.feeds.PandasData):
    """
    A custom data feed for handling Alpaca's data format.
    """

    # Add a 'lines' definition to include VWAP
    lines = ('vwap',)

    # Define the column mapping
    params = (
        ('datetime', 'timestamp'),
        ('open', 'open'),
        ('high', 'high'),
        ('low', 'low'),
        ('close', 'close'),
        ('volume', 'volume'),
        ('vwap', 'vwap'),
        ('openinterest', None),
    )


# Strategy class
class MyStrategy(bt.Strategy):
    params = (
        ('stop_loss', 0.02),  # 2% stop loss per trade
        ('entry_multiplier', 1.01),  # Enter when price is 1% above VWAP
        ('exit_multiplier', 0.99),  # Exit when price is 1% below VWAP
        ('macd_fast', 12),  # Period for the fast moving average
        ('macd_slow', 26),  # Period for the slow moving average
        ('macd_signal', 9),  # Period for the signal line
    )

    def __init__(self):
        self.order = None
        self.macd = bt.indicators.MACD(
            self.data.close,
            period_me1=self.p.macd_fast,
            period_me2=self.p.macd_slow,
            period_signal=self.p.macd_signal
        )

    def next(self):
        if not self.position:
            if self.macd.macd[0] > self.macd.signal[0] and \
                    self.data.close[0] > self.data.vwap[0] * self.p.entry_multiplier:
                self.order = self.buy()
        else:
            if self.macd.macd[0] < self.macd.signal[0] and \
                    self.data.close[0] < self.data.vwap[0] * self.p.exit_multiplier:
                self.order = self.sell()

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                self.sell(exectype=bt.Order.StopTrail, trailpercent=self.p.stop_loss)
            elif order.issell():
                self.buy(exectype=bt.Order.StopTrail, trailpercent=self.p.stop_loss)


class BuyDipsStrategy(bt.Strategy):
    params = (
        ('dip_percentage', 0.001),  # Percentage dip to trigger a buy
        ('buy_amount', 0.05),  # Fraction of available cash to use for each buy
    )

    def __init__(self):
        # To keep track of previous close
        self.previous_close = None

    def next(self):
        if self.previous_close:
            # Calculate the percentage change
            change = (self.data.close[0] - self.previous_close) / self.previous_close * 100

            # Check if the change is more than the dip percentage
            if change <= -self.params.dip_percentage:
                # Calculate the amount to invest
                cash_to_invest = self.broker.get_cash() * self.params.buy_amount
                self.buy(size=cash_to_invest / self.data.close[0])

        # Update the previous close
        self.previous_close = self.data.close[0]


class FractionalSizer(bt.Sizer):
    params = (('buy_amount', 0.1),)  # Default value for buy amount

    def _getsizing(self, comminfo, cash, data, isbuy):
        if isbuy:
            # Implement your logic to calculate the size
            # For example, invest a fixed fraction of total cash
            fraction_of_cash = 0.1  # 10% of cash
            price = data.close[0]
            size = (self.p.buy_amount * cash) / price
            return size
        else:
            # Return default for sell situation
            return self.broker.getposition(data).size
