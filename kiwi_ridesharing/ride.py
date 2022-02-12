from os import kill
import pandas as pd
import numpy as np
from kiwi_ridesharing.data import Kiwi
from kiwi_ridesharing.utils import convert_meters_to_miles

class Ride:
    '''
    DataFrames containing all rides as index,
    and various properties of these rides as columns
    '''
    def __init__(self):
        self.data = Kiwi().get_data()
        self.misc_data = Kiwi().get_misc_data()

    def get_duration_in_minutes(self):

        """
        Returns Dataframe with ride_id, and ride duration in minutes
        """

        rides = self.data["rides"].copy()
        rides["ride_duration_minutes"] = round(rides["ride_duration"]/60)
        return rides[["ride_id", "ride_duration_minutes"]]

    def get_duration_in_hours(self):

        """
        Returns Dataframe with ride_id, and ride duration in hours
        """

        rides = self.data["rides"].copy()
        rides["ride_duration_hours"] = round(rides["ride_duration"]/(60*60))
        return rides[["ride_id", "ride_duration_hours"]]

    def get_speed_kmh(self):

        """
        Returns Dataframe with ride_id, and average speed in kmh
        """
        rides = self.data["rides"].copy()
        rides["average_speed"] = round((rides["ride_distance"]/1000)/(rides["ride_duration"]/(60*60)))
        return rides[["ride_id", "average_speed"]]

    def is_prime_time(self):
        rides = self.data["rides"].copy()
        rides["is_prime_time"] = rides["ride_prime_time"].apply(lambda x: 1 if x > 0 else 0)

        return rides[["ride_id", "ride_prime_time", "is_prime_time"]]

    def get_ride_timestamps(self, clean_data=True):

        """
        Returns Dataframe with ride_id, accepted_at, arrived_at, dropped_off_at,
        picked_up_at, and requested_at as columns

        Parameters:
            clean_data -> bool: indicate weather to replace "arrived_at" timestamp
            with "picked_up_at" timestamp where "arrived_at" > "picked_up_at" and
            where "arrived_at" is Null
        """
        timestamps = self.data["timestamps"].copy()
        timestamps["timestamp"] = pd.to_datetime(timestamps["timestamp"])
        timestamps = timestamps.pivot(values='timestamp', index="ride_id", columns="event").reset_index()
        timestamps.rename_axis(None, axis=1, inplace=True)

        if clean_data:
            timestamps['arrived_at'] = np.where((timestamps['arrived_at'] > timestamps['picked_up_at']) | (timestamps['arrived_at'].isnull()),
                                                timestamps['picked_up_at'], timestamps['arrived_at'])
        return timestamps

    def get_waittime_driver(self):

        """
        Returns Dataframe with ride_id, arrived_at, picked_up_at and
        driver_wait_time in seconds
        """

        wait_time = self.get_ride_timestamps()
        wait_time["driver_wait_time"] = (wait_time["picked_up_at"] - wait_time["arrived_at"]).dt.seconds
        return wait_time[["ride_id", "arrived_at", "picked_up_at", "driver_wait_time"]]

    def get_waittime_customer(self):

        """
        Returns Dataframe with ride_id, arrived_at, picked_up_at and
        customer_wait_time in seconds
        """

        wait_time = self.get_ride_timestamps()
        wait_time["customer_wait_time"] = (wait_time["arrived_at"] - wait_time["accepted_at"]).dt.seconds
        return wait_time[["ride_id", "arrived_at", "picked_up_at", "customer_wait_time"]]


    def get_response_time(self):

        """
        Returns Dataframe with ride_id, requested_at, accepted_at and
        driver_response_time in seconds
        """

        wait_time = self.get_ride_timestamps()
        wait_time["driver_response_time"] = (wait_time["accepted_at"] - wait_time["requested_at"]).dt.seconds

        return wait_time[["ride_id", "arrived_at", "picked_up_at", "driver_response_time"]]


    def get_full_rides_data(self, clean_data=True):

        """
        Returns a DataFrame with the all following columns:
        ['ride_id', 'requested_at', 'accepted_at', 'arrived_at', 'picked_up_at',
        'dropped_off_at', 'ride_duration_minutes', 'ride_duration_hours',
        'average_speed', 'driver_wait_time', 'customer_wait_time']

        Parameters:
            clean_data -> bool: indicate whether to replace "arrived_at" timestamp
            with "picked_up_at" timestamp where "arrived_at" > "picked_up_at" and
            where "arrived_at" is Null

        """

        full_data =\
                self.get_duration_in_minutes()\
                    .merge(
                    self.get_duration_in_hours(), on='ride_id', suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_speed_kmh(), on='ride_id', suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.is_prime_time(), on='ride_id', suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_ride_timestamps(clean_data=clean_data), on='ride_id', how="left", suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_waittime_driver(), on='ride_id', how="left", suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_waittime_customer(), on='ride_id', how="left", suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_response_time(), on='ride_id', how="left", suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.data["rides"][["ride_id", "ride_distance"]], on='ride_id', how="right", suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)")

        def get_fare(ride_duration_minutes, ride_distance_meters, primetime):

            distance_miles = convert_meters_to_miles(ride_distance_meters)

            ride_fare = self.misc_data["base_fare"] +\
                        (self.misc_data["cost_per_mile"] * distance_miles) +\
                        (self.misc_data["cost_per_minute"] * ride_duration_minutes) +\
                        self.misc_data["service_fee"]

            # add primetime bonus
            ride_fare += (primetime/100)*ride_fare

            if ride_fare < 5.00:
                return 5.00

            return round(ride_fare, 2)

        full_data["fare"] = full_data.apply(lambda row: get_fare(row['ride_duration_minutes'],
                                                                 row['ride_distance'],
                                                                 row['ride_prime_time']),
                                            axis=1)

        return full_data[['ride_id',
                          'requested_at',
                          'accepted_at',
                          'arrived_at',
                          'picked_up_at',
                          'dropped_off_at',
                          'ride_duration_minutes',
                          'ride_duration_hours',
                          'ride_distance',
                          'average_speed',
                          'driver_wait_time',
                          'customer_wait_time',
                          'driver_response_time',
                          'fare',
                          'ride_prime_time',
                          'is_prime_time']]
