# -*- coding: utf-8 -*-

import argparse
import os

from ..chat.chatQuery import chatStart

from ..utils.utils import Console
from http.server import BaseHTTPRequestHandler, HTTPServer

if 'nt' == os.name:
    import pyreadline3 as readline
else:
    import readline

    readline.set_completer_delims('')
    readline.set_auto_history(False)

__show_debug = False
class StatusRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'OK')

def start_status_server():
    server_address = ('', 9111)  
    httpd = HTTPServer(server_address, StatusRequestHandler)
    print('Status server started on port 9111')
    httpd.serve_forever()


def main():
    global __show_debug

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-d',
        '--debug',
        help='debug info',
        action='store_true',
    )
    parser.add_argument(
        '-s',
        '--start',
        help='start limit',
        required=False,
        type=int,
        default=0,
    )

    parser.add_argument(
        '-o',
        '--opt',
        help='is opt',
        action='store_true',
    )

    parser.add_argument(
        '-l',
        '--lkey',
        help='lkey',
        action='store_true',
    )

    parser.add_argument(
        '-r',
        '--raw',
        help='raw',
        action='store_true',
    )

    args, _ = parser.parse_known_args()

    __show_debug = args.debug
    start = args.start
      
    access_tokens = ["sk-wuwNArsmS8dIVaxd18AaDdA3912842C4Bc42E5E276D4F53a"]

    chatStart(access_tokens, DEBUG=__show_debug, start = start, opt = args.opt, onekey = args.lkey, raw = args.raw).start_gpt()

def run():
    try:
        main()
    except Exception as e:
        import traceback
        exception_info = traceback.format_exception(type(e), e, e.__traceback__)
        exception_message = ''.join(exception_info)
        
        print("Exception occurred:")
        print(exception_message)
    
        Console.error_bh('### Error occurred: ' + str(e))
