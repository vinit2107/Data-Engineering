import configparser
from configparser import RawConfigParser


class PropertyReader:

    def check_if_empty(self, prop: str, value: str):
        """
            Function to check if the value in the property file is empty
            :param prop: Property name to be validated
            :param value: value of the configured property
            """
        if value.strip() == "":
            print("Property " + prop + " not configured.")
            raise ValueError

    def validate_config(self, config: RawConfigParser):
        """
        Function to validate the configured properties in
        :param config: config object (obtained from RawConfigParser)
        :return:
        """
        print("Validating if the file is configured")
        self.check_if_empty('database.url', config.get('Database', 'database.cluster'))
        self.check_if_empty('database.keyspace', config.get('Database', 'database.keyspace'))

    def readPropertyFile(self):
        """
        Function to read the property file configuration.properties. Returns the RawConfigParser object
        :return: config object (obtained from RawConfigParser)
        """
        print("Reading configuration.properties")
        try:
            config = configparser.RawConfigParser()
            config.read('../configuration.properties')
            self.validate_config(config)
            print("Property file read successfully!")
            return config
        except Exception as ex:
            print("Error reading configuration.properties")
            raise ex
