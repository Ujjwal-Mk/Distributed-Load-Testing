from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from prometheus_client import generate_latest, Counter
import json

# Global counters
counter_requests = Counter('http_server_requests_total', 'Total number of requests received')
counter_responses = Counter('http_server_responses_total', 'Total number of responses sent')

class MyRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global counter_requests, counter_responses

        if self.path == '/ping':
            counter_requests.inc()
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()

            response_message = '{"message": "ping"}'
            print("HELLO WORLD")
            self.wfile.write(response_message.encode('utf-8'))
            counter_responses.inc()
        elif self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain; version=0.0.4')
            self.end_headers()
            
            # Get the current values of the counters
            requests_count = counter_requests._value.get()
            responses_count = counter_responses._value.get()
            
            # Include counter values in the response message
            response_message = f'{{"message": "metrics", "requests_count": {requests_count}, "responses_count": {responses_count}}}'
            self.wfile.write(response_message.encode('utf-8'))
        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/html')
            self.end_headers()

            response_message = 'Not Found'
            self.wfile.write(response_message.encode('utf-8'))

if __name__ == '__main__':
    server_address = ('', 9090)
    httpd = ThreadingHTTPServer(server_address, MyRequestHandler)

    print('Starting the server on port 9090...')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('Server interrupted. Shutting down...')
        httpd.server_close()
