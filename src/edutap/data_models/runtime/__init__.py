"""The Kafka runtime shared by every eduTAP service that consumes a topic.

A loop that hands records to a handler, an error classification, a dead letter queue
and a process that ends when any one consumer does. Two services need exactly this
and a third was about to copy it, which is what moved it here: the behaviour is
contract as much as the header block is -- the commit order, the DLQ naming, what a
stop signal does -- and three copies of a contract diverge.

The line between this package and its consumers is a dependency rule. Everything
here is written against protocols, so ``aiokafka`` stays where the consumers and
producers are actually built: in the service. What a service keeps is
``build_consumer()``, ``build_dead_letter_producer()``, its settings class, and the
knowledge of which schema and which actions a topic allows.
"""

from .consumer import Consumer, DeadLetter, Handler, consume
from .dlq import DeadLetterQueue, Producer, dead_letter_options
from .errors import Unprocessable
from .runner import install_signal_handlers, run_until_one_stops, serve
from .transport import transport_options

__all__ = [
    "Consumer",
    "DeadLetter",
    "DeadLetterQueue",
    "Handler",
    "Producer",
    "Unprocessable",
    "consume",
    "dead_letter_options",
    "install_signal_handlers",
    "run_until_one_stops",
    "serve",
    "transport_options",
]
