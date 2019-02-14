import argparse
import importlib

import requests

_MODULES = [
    'v950901',
    'v959071',
    'v959073',
    'v981173',
    'v981318',
    'v981245',
    'v981248',

    'v950001',
    'v981243',
    'v981242',
    'v981319',
    'v981244',
    'v959072',
    'v981255',

    # Risk Group - total request score inbound
    'v973335',
    'v981249',
    'v959070',
    'v981247',
    'v981320',
    'v981272',
    'v950007',
    'v3000017',
    'v960901',

    # Command Injection
    'v950006',

    # Cross Site Scripting
    'v973332',
    'v973306',
    'v973300',
    'v973300_1',

    # PHP Injection
    'v3000003',

    # Remote File Inclusion
    'v950117',
]


class TestRunner(object):

    def __init__(self, module=None, verbose=False):

        if module is not None:
            self.modules = [module]
        else:
            self.modules = _MODULES

        self.verbose = verbose

    def run(self):

        for modulename in self.modules:
            m = importlib.import_module(modulename)
            self.run_case(m)

    def run_case(self, case):
        print(case.code, case.description)

        s = requests.Session()

        if hasattr(case, 'referer'):
            s.headers.update({'referer': case.referer})

        if hasattr(case, 'query_string') and case.query_string is not None:
            r = s.get(f"{case.uri}?{case.query_string}")
        else:
            r = s.post(case.uri, data=case.postdata)
        print(f"Status = {r.status_code}")

        if self.verbose:
            print(r.url)
            print(r.text)
        print()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--module', type=str)
    parser.add_argument('--verbose', action='store_true')

    args = parser.parse_args()

    o = TestRunner(module=args.module, verbose=args.verbose)
    o.run()
