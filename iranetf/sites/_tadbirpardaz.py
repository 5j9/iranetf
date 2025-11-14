from asyncio import gather
from datetime import date
from io import StringIO
from json import loads
from logging import warning
from re import findall, search, split
from typing import Any

from jdatetime import date as jdate, datetime as jdatetime
from pandas import (
    DataFrame,
    Series,
    concat,
    read_html,
    to_datetime,
    to_numeric,
)

from iranetf import _get
from iranetf.sites._lib import (
    BaseSite,
    LiveNAVPS,
    comma_int,
)

_fa_to_en_tt = str.maketrans(
    {
        '۰': '0',
        '۱': '1',
        '۲': '2',
        '۳': '3',
        '۴': '4',
        '۵': '5',
        '۶': '6',
        '۷': '7',
        '۸': '8',
        '۹': '9',
        ',': '',
    }
)


def _fanum_to_num(series: Series):
    """
    Converts a pandas Series from strings containing Persian digits and commas
    to a numeric data type.
    """
    return to_numeric(series.str.translate(_fa_to_en_tt))


def _comma_float(s: str) -> float:
    return float(s.replace(',', ''))


_jp = jdatetime.strptime


def _jymd_to_greg(date_string, /):
    return _jp(date_string, format='%Y/%m/%d').togregorian()


class TPLiveNAVPS(LiveNAVPS):
    dailyTotalNetAssetValue: int
    dailyTotalUnit: int
    finalCancelNAV: int
    finalEsmiNAV: int
    finalSubscriptionNAV: int
    maxUnit: str
    navDate: str
    nominal: int
    totalNetAssetValue: int
    totalUnit: int
    creation: int
    redemption: int


class BaseTadbirPardaz(BaseSite):
    async def version(self) -> str:
        return (await self.home_info())['version']

    _aa_keys = {
        'اوراق گواهی سپرده',
        'اوراق مشارکت',
        'پنج سهم برتر',
        'سایر دارایی\u200cها',
        'سایر سهام',
        'سایر سهم\u200cها',
        'سهم\u200cهای برتر',
        'شمش و طلا',
        'صندوق سرمایه\u200cگذاری در سهام',
        'صندوق های سرمایه گذاری',
        'نقد و بانک (جاری)',
        'نقد و بانک (سپرده)',
        'گواهی سپرده کالایی',
        'اختیار معامله',
    }

    async def asset_allocation(self) -> dict:
        j: dict = await self._json('Chart/AssetCompositions')
        d = {i['x']: i['y'] / 100 for i in j['List']}
        self._check_aa_keys(d)
        return d

    async def _home_info(self) -> dict[str, Any]:
        html = await self._home()
        d: dict[str, Any] = {
            'isETFMultiNavMode': search(r'isETFMultiNavMode\s*=\s*true;', html)
            is not None,
            'isLeveragedMode': search(r'isLeveragedMode\s*=\s*true;', html)
            is not None,
            'isEtfMode': search(r'isEtfMode\s*=\s*true;', html) is not None,
        }
        if d['isETFMultiNavMode']:
            baskets = findall(
                r'<option [^>]*?value="(\d+)">([^<]*)</option>',
                html.partition('<div class="drp-basket-header">')[2].partition(
                    '</select>'
                )[0],
            )
            d['basketIDs'] = dict(baskets)

        start = html.find('version number:')
        end = html.find('\n', start)
        d['version'] = html[start + 15 : end].strip()

        reg_no_match = search(
            r'<td>شماره ثبت نزد سازمان بورس و اوراق بهادار</td>\s*'
            '<td style="text-align:left">(.*?)</td>',
            html,
        )
        if reg_no_match:
            d['seo_reg_no'] = str(int(reg_no_match[1]))

        return d

    async def cache(self) -> float:
        aa = await self.asset_allocation()
        g = aa.get
        return (
            g('نقد و بانک (سپرده)', 0.0)
            + g('نقد و بانک (جاری)', 0.0)
            + g('اوراق مشارکت', 0.0)
        )

    async def nav_history(
        self, *, from_: date = date(1970, 1, 1), to: date, basket_id=0
    ) -> DataFrame:
        """
        This function uses the HTML output available at /Reports/FundNAVList.
        This is better than the excel output because it includes statistical
        nav column which excel does not have.

        If the output is in multiple pages, this function will fetch them
        all and return the result in a single dataframe.

        Tip: the from_ date can be arbitrary old, e.g. 1900-01-01 but
            there is a 50 year limit on how far in the future the
            `to` can be.
        """
        path = f'Reports/FundNAVList?FromDate={jdatetime.fromgregorian(date=from_):%Y/%m/%d}&ToDate={jdatetime.fromgregorian(date=to):%Y/%m/%d}&BasketId={basket_id}&page=1'
        dfs = []
        while True:
            r = await _get(self.url + path)
            html = (await r.read()).decode()
            # the first table contains regNo which can be ignored
            table = read_html(StringIO(html))[1]
            # the last row is <tfoot> containing next/previous links
            table.drop(table.index[-1], inplace=True)
            dfs.append(table)
            m = search('<a href="([^"]*)" title="Next page">»</a>', html)
            if m is None:
                break
            path = m[1]

        df = concat(dfs, ignore_index=True)
        df.rename(
            columns={
                'ردیف': 'Row',
                'تاریخ': 'Date',
                'قیمت صدور': 'Issue Price',
                'قیمت ابطال': 'Redemption Price',
                'قیمت آماری': 'Statistical Price',
                'NAV صدور واحدهای ممتاز': 'NAV of Premium Units Issued',
                'NAV ابطال واحدهای ممتاز': 'NAV of Premium Units Redeemed',
                'NAV آماری ممتاز': 'Statistical NAV of Premium Units',
                'NAV واحدهای عادی': 'NAV of Normal Units',
                'خالص ارزش صندوق': 'Net Asset Value of Fund',
                'خالص ارزش واحدهای ممتاز': 'Net Asset Value of Premium Units',
                'خالص ارزش واحدهای عادی': 'Net Asset Value of Normal Units',
                'تعداد واحد ممتاز صادر شده': 'Number of Premium Units Issued',
                'تعداد واحد ممتاز باطل شده': 'Number of Premium Units Redeemed',
                'تعداد واحد عادی صادر شده': 'Number of Normal Units Issued',
                'تعداد واحد عادی باطل شده': 'Number of Normal Units Redeemed',
                'مانده گواهی ممتاز': 'Remaining Premium Certificate',
                'مانده گواهی عادی': 'Remaining Normal Certificate',
                'کل واحدهای صندوق': 'Total Fund Units',
                'نسبت اهرمی': 'Leverage Ratio',
                'تعداد سرمایه‌گذاران واحدهای عادی': 'Number of Normal Unit Investors',
                'Unnamed: 21': 'Unnamed_21',
            },
            inplace=True,
        )
        numeric_cols = [
            'Row',
            'Issue Price',
            'Redemption Price',
            'Statistical Price',
            'NAV of Premium Units Issued',
            'NAV of Premium Units Redeemed',
            'Statistical NAV of Premium Units',
            'NAV of Normal Units',
            'Net Asset Value of Fund',
            'Net Asset Value of Premium Units',
            'Net Asset Value of Normal Units',
            'Number of Premium Units Issued',
            'Number of Premium Units Redeemed',
            'Number of Normal Units Issued',
            'Number of Normal Units Redeemed',
            'Remaining Premium Certificate',
            'Remaining Normal Certificate',
            'Total Fund Units',
            'Leverage Ratio',
            'Number of Normal Unit Investors',
        ]
        df[numeric_cols] = df[numeric_cols].apply(_fanum_to_num)
        df['Date'] = df['Date'].map(_jymd_to_greg)
        df.set_index('Date', inplace=True)
        return df


class TadbirPardaz(BaseTadbirPardaz):
    async def live_navps(self) -> TPLiveNAVPS:
        d: str = await self._json('Fund/GetETFNAV')  # type: ignore
        # the json is escaped twice, so it needs to be loaded again
        d: dict = loads(d)  # type: ignore

        d['creation'] = d.pop('subNav')
        d['redemption'] = d.pop('cancelNav')
        d['nominal'] = d.pop('esmiNav')

        for k, t in TPLiveNAVPS.__annotations__.items():
            if t is int:
                try:
                    d[k] = comma_int(d[k])
                except KeyError:
                    warning(f'key {k!r} not found')

        date = d.pop('publishDate')
        try:
            date = jdatetime.strptime(date, '%Y/%m/%d %H:%M:%S')
        except ValueError:
            date = jdatetime.strptime(date, '%Y/%m/%d ')
        d['date'] = date.togregorian()

        return d  # type: ignore

    async def navps_history(self) -> DataFrame:
        j: list = await self._json(
            'Chart/TotalNAV', params={'type': 'getnavtotal'}
        )
        creation, statistical, redemption = [
            [d['y'] for d in i['List']] for i in j
        ]
        date = [d['x'] for d in j[0]['List']]
        df = DataFrame(
            {
                'date': date,
                'creation': creation,
                'redemption': redemption,
                'statistical': statistical,
            }
        )
        df['date'] = to_datetime(df.date)
        df.set_index('date', inplace=True)
        return df

    async def dividend_history(
        self,
        *,
        from_date: date | str | None = None,
        to_date: date | str | None = None,
    ) -> DataFrame:
        params: dict = {'page': 1}
        if from_date is not None or to_date is not None:
            if from_date is not None:
                if isinstance(from_date, date):
                    jd = jdate.fromgregorian(date=from_date)
                    from_date = f'{jd.year}/{jd.month}/{jd.day}'
                params['fromDate'] = from_date
            if to_date is not None:
                if isinstance(to_date, date):
                    jd = jdate.fromgregorian(date=to_date)
                    to_date = f'{jd.year}/{jd.month}/{jd.day}'
                params['toDate'] = to_date

        all_rows = []
        while True:
            html = (
                await (
                    await _get(
                        f'{self.url}Reports/FundDividendProfitReport',
                        params=params,
                    )
                ).read()
            ).decode()
            table, _, after_table = html.partition('<tbody>')[2].rpartition(
                '</tbody>'
            )
            all_rows += [
                findall(r'<td>([^<]*)</td>', r)
                for r in split(r'</tr>\s*<tr>', table)
            ]
            if '" title="Next page">' not in after_table:
                break
            params['page'] += 1

        if not all_rows[0]:  # no data for selected range
            return DataFrame()

        # try to use the same column names as RayanHamafza.dividend_history
        df = DataFrame(
            all_rows,
            columns=[
                'row',
                'ProfitDate',
                'FundUnit',
                'UnitProfit',
                'SUMAllProfit',
                'ProfitPercent',
            ],
        )
        df['ProfitDate'] = df['ProfitDate'].apply(_jymd_to_greg)
        comma_cols = ['FundUnit', 'SUMAllProfit']
        df[comma_cols] = df[comma_cols].map(comma_int)
        int_cols = ['row', 'UnitProfit']
        df[int_cols] = df[int_cols].map(comma_int)
        df['ProfitPercent'] = df['ProfitPercent'].map(_comma_float)
        df.set_index('ProfitDate', inplace=True)
        return df


class TadbirPardazMultiNAV(TadbirPardaz):
    """Same as TadbirPardaz, only send basketId to request params."""

    __slots__ = 'basket_id'

    def __init__(self, url: str):
        """Note: the url ends with #<basket_id> where basket_id is an int."""
        url, _, self.basket_id = url.partition('#')
        super().__init__(url)

    async def _json(self, path: str, params: dict | None = None, **kwa) -> Any:
        return await super()._json(
            path,
            params=(params or {}) | {'basketId': self.basket_id},
            **kwa,
        )


class LeveragedTadbirPardazLiveNAVPS(LiveNAVPS):
    BaseUnitsCancelNAV: float
    BaseUnitsTotalNetAssetValue: float
    BaseUnitsTotalSubscription: int
    SuperUnitsTotalSubscription: int
    SuperUnitsTotalNetAssetValue: float


class LeveragedTadbirPardaz(BaseTadbirPardaz):
    async def navps_history(self) -> DataFrame:
        j: list = await self._json(
            'Chart/TotalNAV', params={'type': 'getnavtotal'}
        )

        append = (frames := []).append

        for i, name in zip(
            j,
            (
                'normal_creation',
                'normal_statistical',
                'normal_redemption',
                'creation',
                'redemption',
                'normal',
            ),
        ):
            df = DataFrame.from_records(i['List'], exclude=['name'])
            df['date'] = to_datetime(df['x'], format='%m/%d/%Y')
            df.drop(columns='x', inplace=True)
            df.rename(columns={'y': name}, inplace=True)
            df.drop_duplicates('date', inplace=True)
            df.set_index('date', inplace=True)
            append(df)

        df = concat(frames, axis=1)
        return df

    async def live_navps(self) -> LeveragedTadbirPardazLiveNAVPS:
        j: str = await self._json('Fund/GetLeveragedNAV')  # type: ignore
        # the json is escaped twice, so it needs to be loaded again
        j: dict = loads(j)  # type: ignore

        pop = j.pop
        date = j.pop('PublishDate')

        result = {}

        for k in (
            'BaseUnitsCancelNAV',
            'BaseUnitsTotalNetAssetValue',
            'SuperUnitsTotalNetAssetValue',
        ):
            result[k] = _comma_float(pop(k))

        result['creation'] = comma_int(pop('SuperUnitsSubscriptionNAV'))
        result['redemption'] = comma_int(pop('SuperUnitsCancelNAV'))

        for k, v in j.items():
            result[k] = comma_int(v)

        try:
            date = jdatetime.strptime(date, '%Y/%m/%d %H:%M:%S')
        except ValueError:
            date = jdatetime.strptime(date, '%Y/%m/%d ')
        result['date'] = date.togregorian()

        return result  # type: ignore

    async def leverage(self) -> float:
        navps, cache = await gather(self.live_navps(), self.cache())
        return (
            1.0
            + navps['BaseUnitsTotalNetAssetValue']
            / navps['SuperUnitsTotalNetAssetValue']
        ) * (1.0 - cache)
