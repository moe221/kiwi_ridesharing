import os
import pandas as pd

class Kiwi:
    def get_data(self):
        """
        This function returns a Python dict.
        Its keys are "timestamps", "drivers", "rides"
        Its values are pandas.DataFrames loaded from the kiwi csv files
        """
        root_dir = os.path.dirname(os.path.dirname(__file__))
        csv_path = os.path.join(root_dir, "kiwi_ridesharing", "data")

        file_names = [f for f in os.listdir(csv_path) if f.endswith(".csv")]
        key_names = ["timestamps", "drivers", "rides"]

        # Create the dictionary
        data = {}
        for k, f in zip(key_names, file_names):
            data[k] = pd.read_csv(os.path.join(csv_path, f))

        return data


    def get_matching_table(self):
        """
        This function returns a matching table between
        columns ["ride_id", "driver_id"]
        """

        matching_table = self.get_data()["rides"][["ride_id", "driver_id"]]
        return matching_table


    def get_misc_data(self):
        """"
        This function returns a Python dict.
        Its keys are "base_fare", "cost_per_mile", "cost_per_minute",
        "service_fee", "min_fare", "max_fare", "driver_commission",
        "kiwi_commission"
        Its values are floats

        """
        misc_data = {"base_fare": 2.0,
                     "cost_per_mile": 1.15,
                     "cost_per_minute": 0.22,
                     "service_fee": 1.75,
                     "min_fare": 5.00,
                     "max_fare": 400,
                     "driver_commission":0.8,
                     "kiwi_fee": 0.2}

        return misc_data
