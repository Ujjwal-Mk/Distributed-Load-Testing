from http.server import BaseHTTPRequestHandler, HTTPServer

class MyRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Respond to GET requests
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

        # Send the response body
        response_message = "Hello World!"
        self.wfile.write(response_message.encode('utf-8'))

if __name__ == '__main__':
    # Set the server address (empty string means localhost) and port
    server_address = ('', 9090)
    httpd = HTTPServer(server_address, MyRequestHandler)
    print('Starting the server on port 9090...')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('Server interrupted. Shutting down...')
        httpd.server_close()