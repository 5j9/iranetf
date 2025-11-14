from iranetf.sites._lib import BaseSite, LiveNAVPS
from iranetf.sites._mabnadp import LeveragedMabnaDP, MabnaDP
from iranetf.sites._rayanhamafza import (
    FundData,
    FundList,
    FundType,
    RayanHamafza,
)
from iranetf.sites._tadbirpardaz import (
    BaseTadbirPardaz,
    LeveragedTadbirPardaz,
    LeveragedTadbirPardazLiveNAVPS,
    TadbirPardaz,
    TadbirPardazMultiNAV,
    TPLiveNAVPS,
)

type AnySite = (
    LeveragedTadbirPardaz
    | TadbirPardaz
    | RayanHamafza
    | MabnaDP
    | LeveragedMabnaDP
)

__all__ = [
    'AnySite',
    'BaseSite',
    'BaseTadbirPardaz',
    'FundData',
    'FundList',
    'FundType',
    'LeveragedMabnaDP',
    'LeveragedTadbirPardaz',
    'LeveragedTadbirPardazLiveNAVPS',
    'LiveNAVPS',
    'MabnaDP',
    'RayanHamafza',
    'TPLiveNAVPS',
    'TadbirPardaz',
    'TadbirPardazMultiNAV',
]
