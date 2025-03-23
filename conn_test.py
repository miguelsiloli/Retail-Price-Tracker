import logging
import sys
import time

# Try to import psycopg2-binary instead of psycopg2
try:
    import psycopg2
except ImportError:
    print("Installing psycopg2-binary...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class PostgresConnector:
    def __init__(
        self,
        connection_type: str = "session_pooler",  # Options: direct, transaction_pooler, session_pooler
        password: str = "4202",
        batch_size: int = 5000
    ):
        """Initialize PostgreSQL connector with connection parameters."""
        self.logger = logging.getLogger(__name__)
        self.batch_size = batch_size
        
        # Define connection strings based on type
        if connection_type == "direct":
            # IPv6 Direct Connection
            self.connection_string = f"postgresql://postgres:{password}@db.ahluezrirjxhplqwvspy.supabase.co:5432/postgres"
            self.connection_description = "Direct Connection (IPv6)"
        elif connection_type == "transaction_pooler":
            # Transaction Pooler (IPv4 compatible)
            self.connection_string = f"postgresql://postgres.ahluezrirjxhplqwvspy:{password}@aws-0-us-east-1.pooler.supabase.com:6543/postgres"
            self.connection_description = "Transaction Pooler (IPv4)"
        elif connection_type == "session_pooler":
            # Session Pooler (IPv4 compatible)
            self.connection_string = f"postgresql://postgres.ahluezrirjxhplqwvspy:{password}@aws-0-us-east-1.pooler.supabase.com:5432/postgres"
            self.connection_description = "Session Pooler (IPv4)"
        else:
            raise ValueError(f"Invalid connection type: {connection_type}. Choose from 'direct', 'transaction_pooler', or 'session_pooler'")
        
    def test_connection(self):
        """Test the connection to the PostgreSQL database."""
        try:
            self.logger.info(f"Attempting to connect to PostgreSQL database using {self.connection_description}...")
            conn = psycopg2.connect(self.connection_string)
            self.logger.info(f"Successfully connected to PostgreSQL database using {self.connection_description}!")
            
            # Get database server version
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            self.logger.info(f"PostgreSQL server version: {version[0]}")
            
            # List available tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tables = cursor.fetchall()
            if tables:
                self.logger.info("Available tables:")
                for table in tables:
                    self.logger.info(f"  - {table[0]}")
            else:
                self.logger.info("No tables found in the public schema.")
            
            # Test basic query execution
            self.logger.info("Testing query execution time...")
            start_time = time.time()
            cursor.execute("SELECT 1;")
            end_time = time.time()
            self.logger.info(f"Query executed in {(end_time - start_time)*1000:.2f} ms")
            
            cursor.close()
            conn.close()
            self.logger.info("Connection closed.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL database: {e}")
            return False

# Test the connection with each method
if __name__ == "__main__":
    # Use command line argument if provided, otherwise default to session_pooler
    if len(sys.argv) > 1 and sys.argv[1] in ["direct", "transaction_pooler", "session_pooler"]:
        connection_type = sys.argv[1]
    else:
        connection_type = "session_pooler"  # Default to IPv4 session pooler
    
    password = "9121759591mM!"  # Replace with your actual password or pass it as an argument
    
    connector = PostgresConnector(connection_type=connection_type, password=password)
    success = connector.test_connection()
    
    # Try alternative methods if the first one fails
    if not success and connection_type != "transaction_pooler":
        print("\nFirst connection method failed. Trying transaction pooler...\n")
        connector = PostgresConnector(connection_type="transaction_pooler", password=password)
        success = connector.test_connection()
    
    sys.exit(0 if success else 1)