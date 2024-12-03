const prometheus = require('prom-client');

class Metrics {

    static order_book_in_count = new prometheus.Counter({
      name: 'order_book_in_count',
      help: 'Counter of received order book updates.',
      labelNames: ['exchange', 'symbol']
    });

    static order_book_in_delay = new prometheus.Histogram({
      name: 'order_book_in_delay',
      help: 'Histogram of received order book delay.',
      buckets: [0.1, 1, 5, 10, 20, 30, 40, 50, 100, 200, 300, 400, 500, 1000]
    });

    static order_book_in_delay_ms = new prometheus.Gauge({
      name: 'order_book_in_delay_ms',
      help: 'Gauge of received order book delay.',
      labelNames: ['exchange', 'symbol']
    });

    static order_book_out_side_price = new prometheus.Gauge({
      name: 'order_book_out_side_price',
      help: 'Gauge of received order book side price.',
      labelNames: ['exchange', 'symbol', 'side']
    });

    static order_book_out_count = new prometheus.Counter({
      name: 'order_book_out_count',
      help: 'Counter of published order book updates.',
      labelNames: ['exchange', 'symbol']
    });

    static order_book_out_delay_ms = new prometheus.Gauge({
      name: 'order_book_out_delay_ms',
      help: 'Gauge of published order book delay.',
      labelNames: ['exchange', 'symbol']
    });
}

module.exports = Metrics