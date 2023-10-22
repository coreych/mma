from moomoo import *

class MoomooApiClient:
    def __init__(self, host, port, api_key, secret_key):
        self.quote_ctx = OpenQuoteContext(host=host, port=port)
        self.api_key = api_key
        self.secret_key = secret_key
        self.handlers = {}  # Store subscription handlers

    def _create_subscription(self, symbols, data_types, subscribe_push=True):
        ret, err_message = self.quote_ctx.subscribe(symbols, data_types, subscribe_push)
        if ret == RET_OK:
            return True, None
        return False, err_message

    def _unsubscribe(self, symbols, data_types):
        ret, err_message = self.quote_ctx.unsubscribe(symbols, data_types)
        if ret == RET_OK:
            return True, None
        return False, err_message

    def add_handler(self, symbol, data_type, handler):
        if symbol not in self.handlers:
            self.handlers[symbol] = {}
        if data_type not in self.handlers[symbol]:
            self.handlers[symbol][data_type] = handler

    def subscribe(self, symbol, data_type):
        success, err_message = self._create_subscription([symbol], [data_type])
        if success:
            print(f'Subscribed to {symbol} ({data_type})')
        else:
            print(f'Subscription failed: {err_message}')

    def unsubscribe(self, symbol, data_type):
        success, err_message = self._unsubscribe([symbol], [data_type])
        if success:
            print(f'Unsubscribed from {symbol} ({data_type})')
        else:
            print(f'Unsubscription failed: {err_message}')

    def handle_subscription(self, rsp_pb):
        symbol = rsp_pb.stock_code
        data_type = rsp_pb.sub_type
        if symbol in self.handlers and data_type in self.handlers[symbol]:
            handler = self.handlers[symbol][data_type]
            handler.on_recv_rsp(rsp_pb)

    def run(self):
        print('Moomoo API Client is running...')
        while True:
            ret, data = self.quote_ctx.get_push_data()
            if ret == RET_OK:
                for rsp_pb in data:
                    self.handle_subscription(rsp_pb)

    def close(self):
        self.quote_ctx.close()

# Example usage:
if __name__ == '__main__':
    api_key = 'your_api_key'
    secret_key = 'your_secret_key'
    client = MoomooApiClient(host='127.0.0.1', port=11111, api_key=api_key, secret_key=secret_key)

    class MyHandler(OrderBookHandlerBase):
        def on_recv_rsp(self, rsp_pb):
            # Your custom handling logic here
            print(f'Received data: {rsp_pb}')

    client.add_handler('HK.00700', SubType.ORDER_BOOK, MyHandler())

    client.subscribe('HK.00700', SubType.ORDER_BOOK)

    try:
        client.run()
    except KeyboardInterrupt:
        client.close()
