import os
import pandas as pd
import numpy as np

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

    def get_competitor_data(self):
        """"
        This function returns two Dataframes with competitor info and
        Geo locations

        """


        areas_dict = {"Auckland": (-36.848461, 174.763336),
                    "Christchurch": (-43.525650, 172.639847),
                    "Dunedin": ( -45.878760, 170.502798),
                    "Hamilton": (-37.78333, 175.28333),
                    "Napier-Hastings": (-39.48333, 176.91667),
                    "Nelson": (-41.27078, 173.28404),
                    "New Plymouth": (-39.06667, 174.08333),
                    "Queenstown": (-45.03023, 168.66271),
                    "Tauranga": (-37.68611, 176.16667),
                    "Wellington": (-41.28664, 174.77557),
                    "Palmerston North": (-40.35636, 175.61113)}


        name = ["Uber", "Zoomy", "Ola", "Kiwi"]
        driver_commision = [0.72, 0.85, 0.82, 0.8]
        base_fare =[1.30, 1.20, 1.65, 2.0]
        service_fee =[0.55, 1.0, 0.48, 1.75]
        cost_per_minute =[0.30, 0.30, 0.38, 0.22]
        cost_per_mile = (np.array([1.35, 1.30, 1.17])*1.609344) # convert to dollars per mile
        cost_per_mile = np.append(cost_per_mile, 1.15)
        min_fare =[6.50, 6.0, 5.81, 5.0]

        df_operators = pd.DataFrame({"operator": name,
                                    "driver_commision": driver_commision,
                                    "base_fare":base_fare,
                                    "service_fee":service_fee,
                                    "cost_per_minute": cost_per_minute,
                                    "cost_per_mile": cost_per_mile,
                                    "min_fare": min_fare})

        geo_df = pd.DataFrame({"name": [*areas_dict.keys()],
                        "lat": [ x[0] for x in areas_dict.values()],
                       "lon": [ x[1] for x in areas_dict.values()]})


        geo_df["operators"] = ["Uber, Ola, Zoomy",
                                "Uber, Ola, Zoomy",
                                "Uber, Ola",
                                "Uber, Ola",
                                "Uber, Ola",
                                "Uber, Ola",
                                "Uber, Ola",
                                "Uber, Ola",
                                "Uber, Ola",
                                "Uber, Ola, Zoomy",
                                "Ola"]
        geo_df["num_operators"] = geo_df["operators"].apply(lambda x: len(x.split(",")))

        return df_operators, geo_df
