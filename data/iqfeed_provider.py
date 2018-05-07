import pandas as pd
from core.logger import get_logger

from functools import partial
from core.contract_store import Store, QuotesType, columns_mapping
from data.data_provider import DataProvider
import pyiqfeed as iq
from core.utility import ConnectionException, cbot_month_code, contract_to_tuple
import datetime

logger = get_logger('iqfeed_provider')


class IQFeedProvider(DataProvider):

    def __init__(self):
        super().__init__()
        self.library = 'iqfeed'

        self.quotes_formats = {QuotesType.futures: self._format_future,
                           QuotesType.currency: self._format_currency,
                           QuotesType.others: self._format_other}
    def connect(self):
        self.hist_conn = iq.HistoryConn(name="pyiqfeed-connection")
        self.hist_listener = iq.VerboseIQFeedListener("History Bar Listener")
        self.hist_conn.add_listener(self.hist_listener)
        self.hist_conn.connect()

        self.lookup_conn = iq.LookupConn(name="pyiqfeed-lookups")
        self.lookup_listener = iq.VerboseIQFeedListener("TickerLookupListener")
        self.lookup_conn.add_listener(self.lookup_listener)
        self.lookup_conn.connect()

        return True

    def disconnect(self):
        self.hist_conn.disconnect()
        self.lookup_conn.disconnect()

    def get_contracts(self, instrument):
        return self.lookup_conn.request_futures_chain(symbol=instrument.iqfeed_symbol, month_codes='FGHJKMNQUVXZ',years='8901234567')


    def download_instrument(self, instrument, **kwagrs):
        ok = self.connect()
        if not ok:
            logger.warning("Download failed: couldn't connect to IQFeed")
            raise ConnectionException("Couldn't connect to IQFeed")

        recent = kwagrs.get('recent', False)
        #try:
        #    contracts = self.get_contracts(instrument)
        #except Exception as e:
        #    logger.warning("Couldn't connect to IQFeed")
        if recent:
            c = instrument.roll_progression().tail(1).iloc[0] - 100 #-100 here rolls it back one year
        else:
            c = str(instrument.first_contract) # Download all contracts
        fail_count = 0
        # we just loop and download them one by one until no data can be found for the next
        logger.info('Downloading contracts for instrument: %s' % instrument.name)
        while fail_count <= 12:
            # print(c)
            if self.download_contract(instrument, c):
                fail_count = 0
            else:
                if int(c/100) >= datetime.datetime.now().year:
                    fail_count += 1
                # Just try one more time in case of a network error
                self.download_contract(instrument, c)
            c = instrument.next_contract(c)
        logger.debug('More than 12 missing contracts in a row - ending the downloading'
                     ' for the instrument %s' % instrument.name)
        self.disconnect()
        return True

    def download_contract(self, instrument, cont_name, **kwagrs):
        year, month = contract_to_tuple(cont_name)
        api_symbol = instrument.iqfeed_symbol + cbot_month_code(month) + str(year)[2:4]
        return self.download_table(QuotesType.futures, instrument.quandl_database,
                                   symbol=api_symbol, db_symbol=instrument.iqfeed_symbol,
                                   instrument=instrument, contract=cont_name)

    def download_currency(self, currency, **kwargs):
        if currency.quandl_symbol[0:3] == currency.quandl_symbol[3:6]:
            return True
        return self.download_table(QuotesType.currency, currency.quandl_database,
                                   currency.quandl_symbol, currency=currency)

    def download_spot(self, spot):
        return self.download_table(QuotesType.others, spot.quandl_database,
                                   spot.quandl_symbol, col=spot.quandl_column)

    def download_table(self, q_type, database, symbol, db_symbol=None, **kwargs):
        # symbol name for the DB storage may be different from what we send to quandl API (e.g. for futures)
        if db_symbol is None:
            db_symbol = symbol

        # specify the format function for the table (depends on quotes type)
        formnat_fn = self.quotes_formats[q_type]
        # for some spot prices the data column is specified explicitly in instruments.py
        # in such cases we pass this column to a format function and save it to database as 'close'
        if 'col' in kwargs.keys():
            formnat_fn = partial(formnat_fn, column=kwargs.get('col'))
        # pass currency object to scale the rate values where needed
        if 'currency' in kwargs.keys():
            formnat_fn = partial(formnat_fn, currency=kwargs.get('currency'))
        # pass spot object to apply multiplier on data format
        if 'spot' in kwargs.keys():
            formnat_fn = partial(formnat_fn, spot=kwargs.get('spot'))
        # pass instrument and contract to format futures data
        if 'instrument' in kwargs.keys():
            formnat_fn = partial(formnat_fn, instrument=kwargs.get('instrument'),
                                 contract=kwargs.get('contract'))

        try:
            logger.info('Downloading %s from IQFeed' % symbol)
            fut_daily = self.hist_conn.request_daily_data(ticker=symbol, num_days=255*3)
            logger.info('Finished downloading %s from IQFeed' % symbol)
            fut_dates = [pair[0] for pair in fut_daily[['date']]]
            fut_open = [pair[0] for pair in fut_daily[['open_p']]]
            fut_high = [pair[0] for pair in fut_daily[['high_p']]]
            fut_low = [pair[0] for pair in fut_daily[['low_p']]]
            fut_close = [pair[0] for pair in fut_daily[['close_p']]]
            fut_volume = [pair[0] for pair in fut_daily[['prd_vlm']]]
            fut_df = pd.DataFrame(data={'open': fut_open, 'high': fut_high, 'low': fut_low, 'close': fut_close, 'volume':fut_volume},
                                      index=pd.DatetimeIndex(data=fut_dates))
            data = fut_df.iloc[::-1]
            store_symbol = '_'.join([kwargs.get('instrument').exchange, db_symbol])
            Store(self.library, q_type, store_symbol).update(formnat_fn(data=data))
            logger.debug('Wrote data for %s/%s' % (database, symbol))
        except (iq.NoDataError, iq.UnauthorizedError) as e:
            logger.warning("NoDataError for symbol {0}".format(symbol))
            return False
        except Exception as e:
            logger.warning('Unexpected error occured: %s' % e)
            return False
        return True

    def drop_symbol(self, q_type, database, symbol, **kwargs):
        Store(self.library, q_type, database + '_' + symbol).delete()

    def drop_instrument(self, instrument):
        self.drop_symbol(QuotesType.futures, instrument.quandl_database, instrument.quandl_symbol)

    def drop_currency(self, currency):
        self.drop_symbol(QuotesType.currency, currency.quandl_database, currency.quandl_symbol)

    def _format_future(self, data, instrument, contract):
        if data is None:
            return None
        data.index = data.index.rename('date')

        data.reset_index(inplace=True)
        data['contract'] = pd.Series(int(contract), index=data.index)
        # .to_datetime() doesn't work here if source datatype is 'M8[ns]'
        # data_out['date'] = data_out['date'].astype('datetime64[ns]')
        return data[['date', 'contract', 'close', 'high', 'low', 'open', 'volume']].copy()

    def _format_currency(self, data, currency):
        if data is None:
            return None
        data.reset_index(inplace=True)
        data.rename(columns=columns_mapping[('quandl', QuotesType.currency.value)], inplace=True)
        data[['rate', 'high', 'low']] = currency.quandl_rate(data[['rate', 'high', 'low']])
        return data[['date', 'rate', 'high', 'low']].copy()

    def _format_other(self, column, data, spot=None):
        if data is None:
            return None
        data.reset_index(inplace=True)
        mapping = columns_mapping[('quandl', QuotesType.others.value)]
        mapping[column] = 'close'
        data.rename(columns=mapping, inplace=True)
        if spot is not None:
            data['close'] *= spot.multiplier
        return data[['date', 'close']].copy()

    def _format_btc(self, data_in):
        raise NotImplementedError
