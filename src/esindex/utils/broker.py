from os import environ

broker = {'host': environ['MHOST'] if 'MHOST' in environ else "localhost",
          'user': environ['MUSER'] if 'MUSER' in environ else "user",
          'provqueue': environ['MPROVQUEUE'] if 'MPROVQUEUE' in environ else "provenance.inbox",
          'rpcqueue': environ['MRPCQUEUE'] if 'MRPCQUEUE' in environ else "attx.indexing.inbox",
          'pass': environ['MKEY'] if 'MKEY' in environ else "password"}
