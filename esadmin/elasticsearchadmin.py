# This module contains utility functions for interacting with the
# Elasticsearch API. 

import httplib
import json
import logging
import math
import re
import sys

logger = logging.getLogger(__name__)

class HttpError(Exception):
    pass

class Connection(object):
    def __init__(self, host, port):
        self.conn = httplib.HTTPConnection(host, port)
        self.es_version = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self):
        self.conn.connect()

    def close(self):
        self.conn.close()

    def request(self, method, path, data=None):
        if data is None:
            self.conn.request(method, path)
        else:
            self.conn.request(method, path, data)

        resp = self.conn.getresponse()
        if resp.status != 200:
            raise HttpError("Expected HTTP 200 - got HTTP %d: %s %s "
                            "(data: %r)" %
                            (resp.status, method, path, data))

        resp_body = resp.read()

        try:
            resp_payload = json.loads(resp_body)
        except ValueError: # Broken JSON
            if len(resp_body) == 0:
                resp_payload = json.loads("{}")
            else:
                logger.error("Failed to parse JSON in response: %r" %
                             resp_body)
                raise

        return resp_payload

    def get(self, path):
        return self.request('GET', path)

    def put(self, path, data):
        return self.request('PUT', path, data)

    def post(self, path, data=""):
        return self.request('POST', path, data)

    def delete(self, path, data=""):
        return self.request('DELETE', path, data)

    def master(self):
        id = self.master_node_id()
        state = self.get('/_cluster/state')
        return state['nodes'][id]['name']

    def master_node_id(self):
        state = self.get('/_cluster/state')
        return state['master_node']

    def my_node_id(self):
        # Elasticsearch 1.0.0 replaced the /_cluster/nodes endpoint
        # with /_nodes. Check which version we're dealing with and
        # choose the correct API.
        if self.es_version is None:
            # Remove everything except digits and . and - separators,
            # turning e.g. 1.0.0-rc1 into 1.0.0-1. The subsequent split
            # on the same separators leaves us with a list of numbers.
            sanitized_version = re.sub(r'[^0-9.-]', '',
                                       self.get('/')['version']['number'])
            self.es_version = tuple(
                int(s) for s in re.split(r'[.-]', sanitized_version))
        if self.es_version >= (1, 0, 0):
            info = self.get('/_nodes/_local')
        else:
            info = self.get('/_cluster/nodes/_local')
        return info['nodes'].keys()[0]

    def indices(self):
        """Return a list of index names in no particular order."""
        state = self.get('/_cluster/state')
        return state['metadata']['indices'].keys()

    def get_index_translog_disable_flush(self):
        """Return a dictionary showing the position of the 
        'translog.disable_flush' knob for each index in the cluster.

        The dictionary will look like this:

            {
                "index1": True,      # Autoflushing DISABLED
                "index2": False,     # Autoflushing ENABLED
                "index3": "unknown", # Using default setting (probably enabled)
                ...
            }

        """
        disabled = {}
        settings = self.get('/_settings')
        for idx in settings:
            idx_settings = settings[idx]['settings']
            disabled[idx] = (booleanise(
                             idx_settings.get('index.translog.disable_flush',
                                              "unknown")))
        return disabled

    def allocator_disabled(self):
        """Return a simplified one-word answer to the question, 'Has the 
        automatic shard allocator been disabled for this cluster?'

        The answer will be one of "disabled" (yes), "enabled" (no), or 
        "unknown".

        """
        state = "unknown"
        k = 'cluster.routing.allocation.disable_allocation'
        settings = self.get('/_cluster/settings')
        for i in ['persistent', 'transient']:
            if k in settings[i]:
                v = booleanise(settings[i][k])
                if v == True:
                    state = "disabled"
                elif v == False:
                    state = "enabled"
        return state

    def flushing_disabled(self):
        """Return a simplified one-word answer to the question, 'Has 
        automatic transaction log flushing been disabled on all indexes in 
        the cluster?'

        The answer will be one of "disabled" (yes, on all), "enabled" (no, 
        on all), "some" (yes, only on some), or "unknown".

        """
        disabled = self.get_index_translog_disable_flush()
        states = set(disabled.values())
        if len(states) > 1 and True in states:
            return "some"
        elif len(states) == 1:
            if True in states:
                return "disabled"
            else:
                return "enabled"
        return "unknown"

class TabularPrinter(object):
    """It prints stuff...  in a tabular format.

    Call row() once for each row you want to print.  Each one of your 
    columns must be supplied to row() as a separate argument.  With all 
    rows queued, call output() to dump out a formatted table.

        tb = TabularPrinter()
        tb.row("foo", "bar")
        tb.row("baz")
        tb.output()

    TabularPrinter will automatically size its columns to suit the 
    supplied content.

    """
    DEFAULT_MARGIN = " " * 4
    DEFAULT_SEPARATOR = "  "

    def __init__(self, margin=DEFAULT_MARGIN,
                 separator=DEFAULT_SEPARATOR):
        self.margin = str(margin)
        self.separator = str(separator)
        self._column_widths = []
        self._max_columns = 0
        self._row_count = 0
        self._rows = []

    @property
    def row_count(self):
        return self._row_count

    def row(self, *columns):
        columns = map(lambda c: str(c), columns)

        self._rows.append(columns)
        self._row_count += 1

        column_widths = map(lambda c: len(c), columns)
        self._column_widths = (
             map(lambda (x, y): max(x, y),
                 self.nontruncating_zip(self._column_widths,
                                        column_widths)))

        self._max_columns = max(self._max_columns, len(columns))

    def output(self, stream=sys.stdout):
        for row in self._rows:
            stream.write(self.margin)

            column_idx = 0
            for column in row:
                fmt_string = "%%-%ds%s" % (
                             self._column_widths[column_idx],
                             self.separator)
                print >>stream, fmt_string % column,
                column_idx += 1

            print >>stream

    @staticmethod
    def nontruncating_zip(*seqs):
        """Return a list of tuples, where each tuple contains the i-th 
        element from each of the argument sequences.

        The returned list is as long as the longest argument sequence.  
        Shorter argument sequences will be represented in the output as 
        None padding elements:

            nontruncating_zip([1, 2, 3], ['a', 'b'])
            -> [(1, 'a'), (2, 'b'), (3, None)]

        """
        n_seqs = len(seqs)

        tups = []
        idx = 0
        while True:
            empties = 0
            tup = []
            for seq in seqs:
                try:
                    tup.append(seq[idx])
                except IndexError:
                    empties += 1
                    tup.append(None)
            if empties == n_seqs:
                break
            tup = tuple(tup)
            tups.append(tup)
            idx += 1

        return tups

def booleanise(b):
    """Normalise a 'stringified' Boolean to a proper Python Boolean.

    ElasticSearch has a habit of returning "true" and "false" in its 
    JSON responses when it should be returning `true` and `false`.  If 
    `b` looks like a stringified Boolean true, return True.  If `b` 
    looks like a stringified Boolean false, return False.

    If we don't know what `b` is supposed to represent, return it back 
    to the caller.

    """
    s = str(b)
    if s.lower() == "true":
        return True
    if s.lower() == "false":
        return False

    return b

def fmt_bytes(bytes, precision=2):
    """Reduce a large number of `bytes` down to a humanised SI 
    equivalent and return the result as a string with trailing unit 
    abbreviation.

    """
    UNITS = ['bytes', 'KB', 'MB', 'GB', 'TB', 'PB']

    if bytes == 0:
        return '0 bytes'

    log = math.floor(math.log(bytes, 1000))

    return "%.*f %s" % (precision,
                       bytes / math.pow(1000, log),
                       UNITS[int(log)])
