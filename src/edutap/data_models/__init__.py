"""Shared contracts of the eduTAP packages.

What lives here is what more than one package must agree on: the controlled
vocabularies, the Kafka message contract and the settings building blocks whose
field names are meant to be identical in every container.

What does not live here is anything a single package owns. Table definitions in
particular stay with their package: ``edutap.db_definitions`` collects them through
entry points, and that collection is what makes one package answerable for one
schema.
"""

from .vocabulary import (
    FieldKind,
    HolderState,
    InstanceState,
    IssuanceState,
    Provider,
    WalletType,
)

# PassLifecycleState is deliberately absent: it is superseded, still reachable
# under edutap.data_models.vocabulary, and warns when it is used.
__all__ = [
    "FieldKind",
    "HolderState",
    "InstanceState",
    "IssuanceState",
    "Provider",
    "WalletType",
]
