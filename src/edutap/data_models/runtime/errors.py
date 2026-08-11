"""The one distinction the error path is built on.

A failure is either **the message's fault** or **the world's**, and the two deserve
opposite treatment. A malformed payload is the same malformed payload on the third
attempt, so retrying it spends the budget on a foregone conclusion. A database that
is briefly away is not a broken message, and parking it would fill the dead letter
topic with perfectly good records during a five second outage.

Everything that is not `Unprocessable` counts as the world's fault. That direction is
deliberate: a new failure nobody has classified yet gets retried and, if it persists,
parked -- which is survivable. The reverse default would discard messages for reasons
nobody has looked at.
"""


class Unprocessable(Exception):
    """A message that cannot be processed, no matter how often it is tried.

    Raised for a payload that is not what it claims to be: a missing mandatory
    header, a timestamp that is not one, an action or schema the consuming service
    does not know. Goes to the dead letter topic on the first attempt.
    """
