"""
Kite Authenticator Module
Handles Zerodha Kite API authentication and provides access token
"""

import json
import os
import webbrowser
import urllib.parse
from kiteconnect import KiteConnect
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv

load_dotenv()

class KiteAuthenticator:
    """
    Kite API Authentication Handler
    """
    
    def __init__(self):
        """Initialize with API credentials"""
        self.api_key = os.environ["KITE_API_KEY"]
        self.api_secret = os.environ["KITE_API_SECRET"]
        
        self.kite = KiteConnect(api_key=self.api_key)
        self.access_token = None
        self.credentials_file = "kite_token.json"
        self.request_token = None
        self.server_port = 8000
    
    def save_token(self, access_token):
        """Save access token to file"""
        token_data = {
            'access_token': access_token,
            'api_key': self.api_key,
            'timestamp': time.time()
        }
        with open(self.credentials_file, 'w') as f:
            json.dump(token_data, f)
        print(f"💾 Token saved to {self.credentials_file}")
    
    def load_token(self):
        """Load saved access token"""
        if os.path.exists(self.credentials_file):
            try:
                with open(self.credentials_file, 'r') as f:
                    data = json.load(f)
                    return data.get('access_token')
            except:
                return None
        return None
    
    def test_token(self, access_token):
        """Test if access token is valid"""
        try:
            temp_kite = KiteConnect(api_key=self.api_key)
            temp_kite.set_access_token(access_token)
            profile = temp_kite.profile()
            return True, profile
        except Exception as e:
            return False, str(e)
    
    def create_redirect_handler(self):
        """Create HTTP handler for catching redirect"""
        auth_instance = self
        
        class RedirectHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                parsed_path = urllib.parse.urlparse(self.path)
                query_params = urllib.parse.parse_qs(parsed_path.query)
                
                if 'request_token' in query_params:
                    auth_instance.request_token = query_params['request_token'][0]
                    
                    # Send success response
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    
                    success_html = """
                    <html>
                        <head><title>Kite Authentication</title></head>
                        <body style="font-family: Arial; text-align: center; margin-top: 100px; background-color: #f0f8ff;">
                            <h2 style="color: green;">✅ Authentication Successful!</h2>
                            <p style="font-size: 18px;">Token received successfully.</p>
                            <p>You can close this browser window now.</p>
                            <p style="color: #666;">Return to your Python application.</p>
                        </body>
                    </html>
                    """
                    self.wfile.write(success_html.encode())
                else:
                    # Send error response
                    self.send_response(400)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    
                    error_html = """
                    <html>
                        <head><title>Kite Authentication Error</title></head>
                        <body style="font-family: Arial; text-align: center; margin-top: 100px; background-color: #ffe4e1;">
                            <h2 style="color: red;">❌ Authentication Failed</h2>
                            <p>Request token not found in URL.</p>
                            <p>Please try again.</p>
                        </body>
                    </html>
                    """
                    self.wfile.write(error_html.encode())
            
            def log_message(self, format, *args):
                # Suppress server logs
                pass
        
        return RedirectHandler
    
    def authenticate_with_server(self):
        """Automated authentication using local server"""
        print("🔐 Starting automated authentication...")
        
        # Start local server
        handler = self.create_redirect_handler()
        server = HTTPServer(('localhost', self.server_port), handler)
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True
        server_thread.start()
        print("🖥️  Local server started on port 8000")
        
        # Open browser for login
        login_url = self.kite.login_url()
        print(f"🌐 Opening browser for login...")
        webbrowser.open(login_url)
        
        print("📱 Please complete login in the browser...")
        print("⏳ Waiting for authentication (timeout: 120 seconds)...")
        
        # Wait for request token
        timeout = 120
        start_time = time.time()
        
        while self.request_token is None:
            if time.time() - start_time > timeout:
                print("⏰ Authentication timeout!")
                server.shutdown()
                return None
            time.sleep(1)
        
        # Stop server
        server.shutdown()
        
        try:
            print(f"✅ Request token received: {self.request_token[:10]}...")
            
            # Generate access token
            session_data = self.kite.generate_session(self.request_token, api_secret=self.api_secret)
            access_token = session_data['access_token']
            
            # Test the token
            is_valid, result = self.test_token(access_token)
            if is_valid:
                self.access_token = access_token
                self.save_token(access_token)
                print(f"🎉 Authentication successful!")
                print(f"👤 User: {result.get('user_name', 'Unknown')}")
                return access_token
            else:
                print(f"❌ Token validation failed: {result}")
                return None
                
        except Exception as e:
            print(f"❌ Authentication error: {e}")
            return None
    
    def get_access_token(self, force_new=False):
        """
        Main method to get access token
        
        Args:
            force_new (bool): Force new authentication even if saved token exists
            
        Returns:
            str: Access token if successful, None if failed
        """
        print("🚀 Kite Authentication Module")
        print("=" * 40)
        
        if not force_new:
            # Try to load saved token first
            saved_token = self.load_token()
            if saved_token:
                print("🔍 Found saved token, testing...")
                is_valid, result = self.test_token(saved_token)
                if is_valid:
                    print(f"✅ Saved token is valid!")
                    print(f"👤 User: {result.get('user_name', 'Unknown')}")
                    self.access_token = saved_token
                    return saved_token
                else:
                    print(f"❌ Saved token expired: {result}")
        
        # Need fresh authentication
        print("🔄 Need fresh authentication...")
        return self.authenticate_with_server()

def get_kite_token(force_new=False):
    """
    Convenience function to get Kite access token
    This is the main function other modules should call
    
    Args:
        force_new (bool): Force new authentication
        
    Returns:
        str: Access token or None
    """
    authenticator = KiteAuthenticator()
    return authenticator.get_access_token(force_new=force_new)

# Test if run directly
if __name__ == "__main__":
    print("Testing Kite Authenticator...")
    token = get_kite_token()
    
    if token:
        print(f"\n✅ SUCCESS!")
        print(f"🔑 Access Token: {token}")
        print(f"\n💡 This token can now be used by other modules")
    else:
        print(f"\n❌ FAILED!")
        print(f"Could not obtain access token")