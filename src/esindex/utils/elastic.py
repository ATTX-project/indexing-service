from os import environ

elastic = {'host': environ['SHOST'] if 'SHOST' in environ else "localhost",
           'port': environ['SPORT'] if 'SPORT' in environ else "9201",
           'index': environ['SINDEX'] if 'SINDEX' in environ else "data"}
