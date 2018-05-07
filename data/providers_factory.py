
def get_provider(name):
    if name == 'quandl':
        from data.quandl_provider import QuandlProvider
        return QuandlProvider()
    elif name == 'ib':
        from data.ib_provider import IBProvider
        return IBProvider()
    elif name == 'iqfeed':
        from data.iqfeed_provider import IQFeedProvider
        return IQFeedProvider()
    else:
        raise Exception('Unknown data provider name: %s' % name)
