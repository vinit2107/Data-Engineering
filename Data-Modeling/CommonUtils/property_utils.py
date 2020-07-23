import configparser


class PropertyUtils:

    def check_if_empty(self, value: str):
        if value.strip() == "":
            raise ValueError

    def validate_config(self, config: configparser.RawConfigParser):
        print("Validating if property file has been configured")
        self.check_if_empty(config.get('Database', 'database.type'))
        self.check_if_empty(config.get('Database', 'database.user'))
        self.check_if_empty(config.get('Database', 'database.password'))
        self.check_if_empty(config.get('Database', 'database.url'))
        self.check_if_empty(config.get('Database', 'database.name'))

    def readfile(self) -> configparser.RawConfigParser:
        print("Reading configuration.properties")
        try:
            config = configparser.RawConfigParser()
            config.read('../configuration.properties')
            self.validate_config(config)
            print("Property file configured...")
            return config
        except Exception as ex:
            message = "An error of type {0} occured."
            print(message.format(type(ex).__name__))
            print("Error reading configuration.properties")
