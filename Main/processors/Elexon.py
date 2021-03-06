# compare for all common tables if static data is complete vs scraped data
# and dump new timeseries data

import pandas as pd
import requests
from tempfile import NamedTemporaryFile
import processors.SQL as SQL


def unit(conn, cur, dumper):
    if unit_test(conn, cur, dumper):
        dumper['area'] = 'Great Britain'
        dumper = get_unit_fuel(dumper)
        # dumper = dumper['fuel']
        dumper = SQL.common(conn, cur, dumper, 'area')
        dumper = SQL.common(conn, cur, dumper, 'fuel')

        unit_type = {'S': 5,
                     'G': 5,
                     'E': 5,
                     'I': 8,
                     'T': 5,
                     'V': 7}

        dumper['unit_type'].replace(unit_type, inplace=True)
        dumper.rename(columns={'unit_type': 'unit_function_id'}, inplace=True)
        dumper = SQL.common(conn, cur, dumper, 'unit')

    else:
        units = SQL.readcommon(conn, cur, 'unit')
        units.rename(columns={'id': 'unit_id'}, inplace=True)
        dumper = pd.merge(
            dumper, units[['unit', 'unit_id']], on='unit', how='left')

    return dumper


def get_unit_fuel(data):

    def get_or_post_a_url(url, post=False, **kwargs):
        """
        Use the requests library to either get or post to a specified URL.
        The return code is checked and exceptions raised if there has been
        a redirect or the status code is not 200.

        :param url: The URL to be used.
        :param post: True if the request should be a POST. Default is False which results in a
        GET request.
        :param kwargs: Optional keyword arguments that are passed directly to the requests call.
        :returns: The requests object is returned if all checks pass.
        :rtype: :class:`requests.Response`
        :raises: Raises :exc:`Exception` for various errors.

        .. :note:: Normally the returned URL is compared with the URL requested. In cases \
        where this may change using the :param:ignore_url_check=True parameter will avoid this \
        check. It will not be passed to requests.

        Example

        .. :code:: python

        >>> from pywind.utils import get_or_post_a_url
        >>> qry = {'q': 'rst document formatting'}
        >>> response = get_or_post_a_url('http://www.google.com/search', params=qry)
        >>> response.content
        ...

        """
        ignore_req_check = kwargs.pop('ignore_url_check', False)

        try:
            if post:
                req = requests.post(url, **kwargs)
            else:
                req = requests.get(url, **kwargs)
        except requests.exceptions.SSLError as err:
            raise Exception("SSL Error\n  Error: {}\nURL: {}".
                            format(err.message[0], url))
        except requests.exceptions.ConnectionError:
            raise Exception("Unable to connect to the server.\nURL: {}".
                            format(url))
        if req.status_code != 200:
            raise Exception("Request was completed, but status code is not 200.\n" +
                            "URL: {}\nStatus Code: {}".format(url, req.status_code))

        if ignore_req_check is False and req.url != url:
            if 'params' not in kwargs or not req.url.startswith(url):
                raise Exception("Returned URL was from a different URL than requested.\n" +
                                "Requested: {}\nActual:  {}".format(url, req.url))
        return req

    resp = get_or_post_a_url(
        'https://www.bmreports.com/bmrs/cloud_doc/BMUFuelType.xls')

    tmp_f = NamedTemporaryFile(delete=False)
    with open(tmp_f.name, 'wb') as fhh:
        fhh.write(resp.content)

    fuel = pd.read_excel(tmp_f.name)

    fuel.rename(columns={'NGC_BMU_ID': 'unit',
                         'FUEL TYPE': 'fuel'}, inplace=True)

    fuel.loc[:, 'fuel'] = fuel.fuel.str.upper()
    # print(data.head())
    # print(fuel.head())
    data = pd.merge(data, fuel, on='unit', how='left')
    data['fuel'].fillna(value='Unknown', inplace=True)
    return data


def unit_test(conn, cur, data):
    # check if there are any new units
    new = False
    cunit = SQL.readcommon(conn, cur, 'unit')
    units = pd.DataFrame(data['unit'].unique(), columns=['unit'])
    units = pd.merge(units, cunit, on='unit', how='left')
    units = units[pd.isnull(units.id)][['unit']]
    # print(units.head())
    if not units.empty:
        new = True
    return new
