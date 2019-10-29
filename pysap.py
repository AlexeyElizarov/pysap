from pyrfc import Connection


class SAP:
    languages = ['RU', 'EN']

    def __init__(self, profile):
        self.connection = self._connect(profile)

    @staticmethod
    def _read_profile(profile: str):
        """
        Reads connection profile. Connection parameters consist of client, user, passwd, ahost and sysnr.
        :param profile: path to SAP  connection profile.
        :return: dictionary with connection parameters
        """

        params = {}

        with open(profile, mode='r') as f:

            for param in f.readlines():
                key, value = param.split('=')
                params[key.strip()] = value.strip()

        return params

    def _connect(self, profile):
        """
        Provides connection to an SAP backend system
        :param profile: path to an SAP connection profile.
        :return: pyrfc.Connection (https://sap.github.io/PyRFC/pyrfc.html)
        """
        return Connection(**self._read_profile(profile))

    def read_table(self, query_table: str,
                   delimiter: str = '|',
                   no_data: str = '',
                   rowskips: int = 0,
                   rowcount: int = 0,
                   options: str = '',
                   fields: list = None):
        """
        Invokes RFC_READ_TABLE function module via RFC.
        :param query_table: Table to read.
        :param delimiter: Sign for indicating field limits in DATA. Default "|".
        :param no_data: If <> SPACE, only FIELDS is filled
        :param rowskips: Skips certain number of rows.
        :param rowcount: Number of rows to read.
        :param options: Selection entries, "WHERE clauses" .
        :param fields: Names (in) and structure (out) of fields to read.
        :return: tuple of data and fields
        """

        _options = [{'TEXT': options.replace('\"', '\'')}]

        if fields is None:
            _fields = []
        else:
            _fields = [{'FIELDNAME': field} for field in fields]

        response = self.connection.call('RFC_READ_TABLE',
                                        QUERY_TABLE=query_table,
                                        DELIMITER=delimiter,
                                        NO_DATA=no_data,
                                        ROWSKIPS=rowskips,
                                        ROWCOUNT=rowcount,
                                        OPTIONS=_options,
                                        FIELDS=_fields)

        _data, _fields = response['DATA'], response['FIELDS']
        _field_names = [field['FIELDNAME'] for field in _fields]

        data = list()

        if _data:
            for item in _data:
                wa = item['WA'].split('|')
                attribs = {key: value.strip() for (key, value) in zip(_field_names, wa)}
                data.append(attribs)

        fields = _fields

        return data, fields
