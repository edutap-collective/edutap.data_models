"""The controlled values every eduTAP package shares.

These supersede the three divergent copies that grew separately: eight values in
``lmu_edutap_common.PassStatus``, six in ``lmu_edutap_full_view`` and six in
``edutap.data_provider``. The spellings below follow the data provider's, which was
already documented as the leading one.

A consumer that does not otherwise depend on this package is welcome to copy the
values rather than import them -- an import would point its dependency at a package
it has no other reason to know.
"""

from enum import StrEnum


class WalletType(StrEnum):
    """Which wallet technology a pass was issued for."""

    GOOGLE_ST = "GOOGLE_ST"
    GOOGLE_ACCESS = "GOOGLE_ACCESS"
    APPLE_VAS = "APPLE_VAS"
    APPLE_ACCESS = "APPLE_ACCESS"
    APPLE_IDENTITY = "APPLE_IDENTITY"
    SAMSUNG_ST = "SAMSUNG_ST"
    SAMSUNG_ACCESS = "SAMSUNG_ACCESS"


class PassLifecycleState(StrEnum):
    """Where a pass stands in its life.

    Stored and delivered, never validated on the way through. Note that this is the
    *issuer* axis; whether a pass has reached its holder is a separate question with
    a separate answer -- see the pass state model design record.
    """

    NEW = "NEW"
    INSTALL_PENDING = "INSTALL_PENDING"
    UPDATE_PENDING = "UPDATE_PENDING"
    DELETE_PENDING = "DELETE_PENDING"
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


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
