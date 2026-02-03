import earthaccess
import os

class EarthdataAuth:
    """
    Base class for handling NASA Earthdata authentication.
    """

    def __init__(self):
        """Initialize the authentication handler."""
        self.username = os.environ.get("EARTHDATA_USERNAME")
        self.password = os.environ.get("EARTHDATA_PASSWORD")

    def authenticate(self):
        """
        Perform authentication with NASA Earthdata.
        Returns:
            bool: True if authentication is successful, False otherwise.
        """
        print("[Auth] Initializing NASA Earthdata Authentication...")
        try:
            auth = earthaccess.login(strategy="interactive", persist=True)
            if auth.authenticated:
                print(f" [Auth] Logged in successfully.")
                return True
            return False
        except Exception as e:
            print(f" [Auth] Authentication failed: {e}")
            return False

    def get_session(self):
        """
        Returns an authenticated requests session.
        Inheriting classes can use this without importing 'earthaccess'.
        """
        return earthaccess.get_requests_https_session()