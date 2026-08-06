"""Shared contracts of the eduTAP packages.

What lives here is what more than one package must agree on: the controlled
vocabularies, the Kafka message contract and the settings building blocks whose
field names are meant to be identical in every container.

What does not live here is anything a single package owns. Table definitions in
particular stay with their package: ``edutap.db_definitions`` collects them through
entry points, and that collection is what makes one package answerable for one
schema.
"""

from .vocabulary import FieldKind, PassLifecycleState, Provider, WalletType

__all__ = ["FieldKind", "PassLifecycleState", "Provider", "WalletType"]
