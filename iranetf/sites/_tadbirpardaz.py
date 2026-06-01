from asyncio import gather
from datetime import date
from json import loads
from re import findall, search, split
from typing import Any

import polars as pl
from jdatetime import date as jdate, datetime as jdatetime

from iranetf import _get
from iranetf.sites._lib import (
    BaseSite,
    LiveNAVPS,
    comma_int,
    reg_no_from_home_info,
)

# Optimized vector replacement map for stripping Persian characters
_FA_TO_EN_REPLACEMENTS = {
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


def _clean_persian_numeric_expr(column_name: str) -> pl.Expr:
    """
    Returns a Polars expression that strips Persian numerals and commas
    from a string column, then casts it to Float64 natively.
    """
    expr = pl.col(column_name).str.strip_chars()
    for fa_char, en_char in _FA_TO_EN_REPLACEMENTS.items():
        expr = expr.str.replace_all(fa_char, en_char)
    return expr.cast(pl.Float64)


def _jymd_to_greg(date_string: str | None) -> Any:
    if date_string is None:
        return None
    return jdatetime.strptime(date_string, '%Y/%m/%d').togregorian()


def _comma_float(s: str) -> float:
    return float(s.replace(',', ''))


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
    reg_no = reg_no_from_home_info

    async def version(self) -> str:
        return (await self.home_info())['version']

    _aa_keys = {
        'اوراق گواهی سپرده',
        'اوراق مشارکت',
        'پنج سهم برتر',
        'سایر دارایی‌ها',
        'سایر سهام',
        'سایر سهم‌ها',
        'سهم‌های برتر',
        'شمش و طلا',
        'صندوق سرمایه‌گذاری در سهام',
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
            r'<td>شماره ثبت نزد سازمان بورس و اوراق بهادار</td>\s*<td style="text-align:left">(.*?)</td>',
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
    ) -> pl.LazyFrame:
        """
        Fetches historical NAV raw matrix blocks via automated page streaming loops.
        """
        path = f'Reports/FundNAVList?FromDate={jdatetime.fromgregorian(date=from_):%Y/%m/%d}&ToDate={jdatetime.fromgregorian(date=to):%Y/%m/%d}&BasketId={basket_id}&page=1'
        all_pages_data = []

        while True:
            r = await _get(self.url + path)
            html = (await r.read()).decode()

            # Isolate the data grid row segment cleanly without requiring pandas `read_html`
            table_body = html.partition('<tbody>')[2].partition('</tbody>')[0]
            rows = split(r'</tr>\s*<tr>', table_body)

            for row in rows:
                cells = findall(r'<td>([^<]*)</td>', row)
                if cells:
                    all_pages_data.append(cells)

            m = search('<a href="([^"]*)" title="Next page">»</a>', html)
            if m is None:
                break
            path = m[1]

        ordered_columns = [
            'Row',
            'Date',
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

        target_len = len(ordered_columns)
        cleaned_rows = []

        for row in all_pages_data:
            if len(row) < target_len:
                # Safe text-padding boundary alignment
                padded_row = row + [''] * (target_len - len(row))
                cleaned_rows.append(padded_row)
            else:
                cleaned_rows.append(row[:target_len])

        if not cleaned_rows:
            return pl.LazyFrame(
                [], schema={col: pl.String for col in ordered_columns}
            )

        lf = pl.LazyFrame(cleaned_rows, schema=ordered_columns, orient='row')

        numeric_cols = [col for col in ordered_columns if col != 'Date']

        # Pre-process the columns: convert explicit empty strings to structural null values.
        # This prevents the downstream .strict_cast(Float64) from throwing format errors.
        normalized_lf = lf.with_columns(
            [
                pl.when(pl.col(col) == '')
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
                for col in numeric_cols
            ]
        )

        # Execute your native conversion pipelines on the sanitized structural matrix
        return normalized_lf.with_columns(
            [
                _clean_persian_numeric_expr(col).alias(col)
                for col in numeric_cols
            ]
            + [
                pl.col('Date').map_elements(
                    _jymd_to_greg, return_dtype=pl.Datetime
                )
            ]
        )

    async def portfolios(self) -> dict[str, str]:
        home_info = await self.home_info()
        if home_info['isETFMultiNavMode']:
            baskets = home_info['basketIDs']
            baskets.pop('1', None)
            return baskets
        return {'1': self.url}


class TadbirPardaz(BaseTadbirPardaz):
    async def live_navps(self) -> TPLiveNAVPS:
        d_raw: str = await self._json('Fund/GetETFNAV')
        d: dict = loads(d_raw)

        d['creation'] = d.pop('subNav')
        d['redemption'] = d.pop('cancelNav')
        d['nominal'] = d.pop('esmiNav')

        for k, t in TPLiveNAVPS.__annotations__.items():
            if t is int and k in d:
                d[k] = comma_int(d[k])

        date_str = d.pop('publishDate')
        try:
            parsed_date = jdatetime.strptime(date_str, '%Y/%m/%d %H:%M:%S')
        except ValueError:
            parsed_date = jdatetime.strptime(date_str, '%Y/%m/%d ')
        d['date'] = parsed_date.togregorian()

        return d  # type: ignore

    async def navps_history(self) -> pl.LazyFrame:
        j: list = await self._json(
            'Chart/TotalNAV', params={'type': 'getnavtotal'}
        )
        creation = [d['y'] for d in j[0]['List']]
        statistical = [d['y'] for d in j[1]['List']]
        redemption = [d['y'] for d in j[2]['List']]
        date_list = [d['x'] for d in j[0]['List']]
        lf = pl.LazyFrame(
            {
                'date': date_list,
                'creation': creation,
                'redemption': redemption,
                'statistical': statistical,
            }
        )
        return lf.with_columns(pl.col('date').str.to_datetime('%m/%d/%Y'))

    async def dividend_history(
        self,
        *,
        from_date: date | str | None = None,
        to_date: date | str | None = None,
    ) -> pl.LazyFrame:
        params: dict = {'page': 1}
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

            for r in split(r'</tr>\s*<tr>', table):
                cells = findall(r'<td>([^<]*)</td>', r)
                if cells:
                    all_rows.append(cells)

            if '" title="Next page">' not in after_table:
                break
            params['page'] += 1

        if not all_rows or not all_rows[0]:
            return pl.LazyFrame([], schema={'profitDate': pl.Datetime})

        lf = pl.LazyFrame(
            all_rows,
            schema=[
                'row',
                'profitDate',
                'fundUnit',
                'unitProfit',
                'sumAllProfit',
                'profitPercent',
            ],
            orient='row',
        )

        lf = lf.with_columns(
            pl.col('profitPercent').str.strip_chars().replace('∞', None)
        )

        # Vectorized translation expressions replacing map/apply configurations
        return lf.lazy().with_columns(
            [
                pl.col('profitDate').map_elements(
                    _jymd_to_greg, return_dtype=pl.Datetime
                ),
                _clean_persian_numeric_expr('fundUnit').cast(pl.Int64),
                _clean_persian_numeric_expr('sumAllProfit').cast(pl.Int64),
                _clean_persian_numeric_expr('row').cast(pl.Int64),
                _clean_persian_numeric_expr('unitProfit').cast(pl.Int64),
                _clean_persian_numeric_expr('profitPercent'),
            ]
        )


class TadbirPardazMultiNAV(TadbirPardaz):
    __slots__ = 'basket_id'

    def __init__(self, url: str):
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
    async def navps_history(self) -> pl.LazyFrame:
        j: list = await self._json(
            'Chart/TotalNAV', params={'type': 'getnavtotal'}
        )

        names = (
            'normal_creation',
            'normal_statistical',
            'normal_redemption',
            'creation',
            'redemption',
            'normal',
        )

        combined_df: pl.LazyFrame | None = None

        for i, name in zip(j, names):
            # Parse record tracks efficiently using native dataframe allocations
            lf = (
                pl.LazyFrame(i['List'])
                .select(
                    [
                        pl.col('x')
                        .str.to_datetime('%m/%d/%Y', strict=False)
                        .alias('date'),
                        pl.col('y').cast(pl.Float64).alias(name),
                    ]
                )
                .unique(subset=['date'])
            )

            if combined_df is None:
                combined_df = lf
            else:
                combined_df = combined_df.join(
                    lf, on='date', how='full', coalesce=True
                )

        if combined_df is None:
            # Return empty LazyFrame with proper schema
            schema: dict = {'date': pl.Date}
            for name in names:
                schema[name] = pl.Float64
            return pl.LazyFrame(schema=schema)

        return combined_df

    async def live_navps(self) -> LeveragedTadbirPardazLiveNAVPS:
        j_raw: str = await self._json('Fund/GetLeveragedNAV')
        j: dict = loads(j_raw)

        pop = j.pop
        date_str = j.pop('PublishDate')

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
            parsed_date = jdatetime.strptime(date_str, '%Y/%m/%d %H:%M:%S')
        except ValueError:
            parsed_date = jdatetime.strptime(date_str, '%Y/%m/%d ')
        result['date'] = parsed_date.togregorian()

        return result  # type: ignore

    async def leverage(self) -> float:
        navps, cache = await gather(self.live_navps(), self.cache())
        return (
            1.0
            + navps['BaseUnitsTotalNetAssetValue']
            / navps['SuperUnitsTotalNetAssetValue']
        ) * (1.0 - cache)
