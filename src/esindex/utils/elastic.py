from os import environ

elastic = {'host': environ['ESHOST'] if 'ESHOST' in environ else "localhost",
           'port': environ['ESPORT'] if 'ESPORT' in environ else "9210",
           'bulkport': environ['ESPORTB'] if 'ESPORTB' in environ else "9310"}
