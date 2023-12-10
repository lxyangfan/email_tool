import platform

default_encoding='utf-8'
windows_encoding='mbcs'
csv_encoding= windows_encoding if platform.system() == 'Windows' else default_encoding