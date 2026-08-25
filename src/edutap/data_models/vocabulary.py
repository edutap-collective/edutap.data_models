"""The controlled values every eduTAP package shares.

These supersede the three divergent copies that grew separately: eight values in
``lmu_edutap_common.PassStatus``, six in ``lmu_edutap_full_view`` and six in
``edutap.data_provider``. The spellings below follow the data provider's, which was
already documented as the leading one.

A consumer that does not otherwise depend on this package is welcome to copy the
values rather than import them -- an import would point its dependency at a package
it has no other reason to know.

The pass lifecycle is spelled on **three** axes, not one. Which axis a value belongs
to is decided by who can observe it: the issuer knows what it did, the platform
reports what one exemplar is doing, and what the holder has is the summary of the
second. Pressing them into one column is what made every earlier vocabulary
unusable, and the reasoning is written up in the pass state model design record.
"""

import warnings
from enum import StrEnum


class WalletType(StrEnum):
    """Which wallet technology a pass was issued for."""

    GOOGLE_ST = "GOOGLE_ST"
    GOOGLE_ACCESS = "GOOGLE_ACCESS"
    GOOGLE_IDENTITY = "GOOGLE_IDENTITY"
    APPLE_VAS = "APPLE_VAS"
    APPLE_ACCESS = "APPLE_ACCESS"
    APPLE_IDENTITY = "APPLE_IDENTITY"
    SAMSUNG_ST = "SAMSUNG_ST"
    SAMSUNG_ACCESS = "SAMSUNG_ACCESS"
    SAMSUNG_IDENTITY = "SAMSUNG_IDENTITY"
    EUDI_PASS = "EUDI_PASS"


class IssuanceState(StrEnum):
    """What the issuer did or wants -- the axis fully under our own control.

    It exists without any exemplar at all: a pass issued and never installed is
    ``ISSUED``, a withdrawn one is ``REVOKED``, whatever sits on the devices. That
    independence is the reason it cannot be derived from the instances.

    Deliberately absent are ``INSTALL_PENDING``, ``UPDATE_PENDING`` and
    ``DELETE_PENDING``: they describe a provisioning run, which exists at Apple
    Access and nowhere else. A ``.pkpass`` has no "install pending", and neither
    does a Google object.
    """

    CREATED = "CREATED"
    ISSUED = "ISSUED"
    REVOKED = "REVOKED"
    EXPIRED = "EXPIRED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class HolderState(StrEnum):
    """Whether the pass has reached its holder -- derived, never set by a caller.

    ``PRESENT`` when at least one instance is ``ACTIVE``, ``SUSPENDED`` when
    instances exist but none is active and one is suspended, ``NOT_PRESENT``
    otherwise. It is stored rather than computed on read, which only holds as long
    as a single writer maintains it in the same transaction as the instance.
    """

    NOT_PRESENT = "NOT_PRESENT"
    PRESENT = "PRESENT"
    SUSPENDED = "SUSPENDED"


class InstanceState(StrEnum):
    """What one exemplar of a pass at the holder is doing.

    What an exemplar *is* the platform decides: a device registration or a
    provisioned credential at Apple, the save into the account at Google.

    Not every platform can confirm every value. Apple VAS cannot distinguish
    ``FAILED`` from "not installed yet" -- both look like the absence of a
    registration -- and neither Apple VAS nor Google reports ``SUSPENDED`` at all.
    A report built on those values has to say so.
    """

    PROVISIONING = "PROVISIONING"
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    REMOVED_BY_HOLDER = "REMOVED_BY_HOLDER"
    REMOVED_BY_ISSUER = "REMOVED_BY_ISSUER"
    FAILED = "FAILED"


class FieldKind(StrEnum):
    """What a field is good for -- not what it holds."""

    STRING = "STRING"
    TEXT = "TEXT"
    DATETIME = "DATETIME"
    LINK = "LINK"
    NFC = "NFC"
    BARCODE = "BARCODE"
    IMAGE = "IMAGE"


class Provider(StrEnum):
    """The wallet provider behind a pass."""

    APPLE = "APPLE"
    GOOGLE = "GOOGLE"
    SAMSUNG = "SAMSUNG"
    EUDI = "EUDI"


class PassLifecycleState(StrEnum):
    """Superseded by the three-axis vocabulary above.

    Replaced by :class:`IssuanceState`, :class:`HolderState` and
    :class:`InstanceState`. It conflates the two axes the split exists to separate,
    and its provisioning steps are Apple-Access-shaped. Kept reachable so a package
    importing it is told, instead of quietly receiving a different set of values
    than the one the database now holds.
    """

    NEW = "NEW"
    INSTALL_PENDING = "INSTALL_PENDING"
    UPDATE_PENDING = "UPDATE_PENDING"
    DELETE_PENDING = "DELETE_PENDING"
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


# Removing the module-level binding is what routes the name through __getattr__
# below; the class object itself survives in the mapping and keeps its own __name__,
# so its repr still reads PassLifecycleState rather than some shim's name.
_SUPERSEDED = {"PassLifecycleState": PassLifecycleState}
del PassLifecycleState


def __getattr__(name: str) -> object:
    """Serve a superseded name once, with a warning, and nothing else.

    A module-level hook rather than a ``warnings.deprecated`` decorator, and not
    because of the Python floor: PEP 702 makes a decorated *class* warn when it is
    instantiated or subclassed, and an enum member is neither. ``ACTIVE`` would go
    through in silence, which is the one access this shim exists to catch. The hook
    fires on the import instead -- the moment a package commits to the old spelling.

    Anything else has to keep raising ``AttributeError`` -- a hook that answers every
    name turns a typo into a warning and a value.
    """
    if name in _SUPERSEDED:
        warnings.warn(
            f"{name} is superseded by IssuanceState, HolderState and InstanceState; "
            "it conflates the issuer and holder axes and will be removed.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _SUPERSEDED[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
