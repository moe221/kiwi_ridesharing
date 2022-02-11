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
