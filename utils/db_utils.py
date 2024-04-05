import yaml
import sqlalchemy

class AWSDBConnector:
    def __init__(self, yaml_file_path='db_creds.yaml'):
        """
        Initializes the AWSDBConnector with database connection parameters.

        Parameters:
        yaml_file_path (str): Path to the YAML file containing database credentials.
        """
        self.db_creds = self.read_db_creds(yaml_file_path)
        
    def read_db_creds(self, yaml_file_path) -> dict:
        """
        Reads the contents of a YAML file containing database credentials and
        returns them as a dictionary.

        Parameters:
        yaml_file_path (str): Path to the YAML file containing database credentials.

        Returns:
        dict: Dictionary containing database credentials.
        """
        try:
            with open(yaml_file_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise Exception(f"YAML file {yaml_file_path} not found")
        except yaml.YAMLError as e:
            raise Exception(f"Error reading YAML file: {e}")
    
    def create_db_connector(self):
        """
        Creates a database engine using SQLAlchemy.

        Returns:
        engine: A SQLAlchemy engine instance connected to the database.
        """
        USER = self.db_creds['USER']
        PASSWORD = self.db_creds['PASSWORD']
        HOST = self.db_creds['HOST']
        PORT = self.db_creds['PORT']
        DATABASE = self.db_creds['DATABASE']
        
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?charset=utf8mb4")
        return engine
