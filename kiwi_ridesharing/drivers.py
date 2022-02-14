import pandas as pd
import numpy as np
import sqlite3
from kiwi_ridesharing.data import Kiwi
from kiwi_ridesharing.ride import Ride
import os

root_dir = os.path.dirname(os.path.dirname(__file__))
db_path = os.path.join(root_dir, "kiwi_ridesharing", "data")

conn = sqlite3.connect(os.path.join(db_path, 'kiwi_datawarehouse.db'))
db = conn.cursor()

class Driver:
    '''
    DataFrames containing all rides as index,
    and various properties of these rides as columns
    '''
    def __init__(self):
        self.data = Kiwi().get_data()
        self.misc_data = Kiwi().get_misc_data()
        self.matching_table = Kiwi().get_matching_table()
        self.rides = Ride().get_full_rides_data()


    def _get_first_last_trip(self):

        """
        Function that returns a Dataframe with "driver_id",
        "driver_onboard_date", "first_ride" and "last_ride" timestamp
        """

        drivers = self.data["drivers"].copy()

        # convert driver_onboard_date to datetime
        drivers["driver_onboard_date"] = pd.to_datetime(drivers["driver_onboard_date"])

        # match ride id whith driver id
        rides = self.rides.merge(self.matching_table, on="ride_id", how="left")
        # sort by driver id and dropped_off_at
        rides_sorted = rides.sort_values(by=["driver_id", "dropped_off_at"], ascending=True)
        # remove duplicate driver id and keep ones with the first dropped off timestamp
        first_ride = rides_sorted.drop_duplicates("driver_id", keep="first")[["driver_id", "dropped_off_at"]]
        last_ride = rides_sorted.drop_duplicates("driver_id", keep="last")[["driver_id", "dropped_off_at"]]

        # join with drivers table
        first_last_trip = \
            drivers.merge(first_ride, how="left", on="driver_id").\
                merge(last_ride, how="left", on="driver_id")

        # rename columns
        first_last_trip.columns = ["driver_id", "driver_onboard_date", "first_ride", "last_ride"]

        return first_last_trip


    def get_lifetime(self):

        """
        Function that returns Dataframe of drivers and number of days they
        drove for Kiwi - days between onboarding and last ride
        """

        lifetime = self._get_churn().copy()

        last_timestamp_in_kiwi_database = self.rides["dropped_off_at"].max()

        # if driver churned, then lifetime is the number of days between onboardning and last_ride timestamp
        # otherwise, days between onboarding and last ride timestamp in kiwi database is taken
        lifetime["lifetime"] = np.where(lifetime["is_churn"]==1,
                                        (lifetime["last_ride"] - lifetime["driver_onboard_date"]).dt.days,
                                        (last_timestamp_in_kiwi_database - lifetime["driver_onboard_date"]).dt.days)

        return lifetime

    def get_days_between_rides(self):

        """
        Function that returns a Dataframe of driver ids and the max number of consecutive days between rides
        """

        query = """

                WITH x AS(
                        SELECT ride_ids.driver_id, ride_timestamps.event,
                        ride_timestamps.timestamp, LAG(ride_timestamps.timestamp) OVER(
                            PARTITION BY driver_id
                            ORDER BY timestamp
                            ) AS offsetDate FROM ride_ids
                            LEFT JOIN ride_timestamps ON
                            ride_ids.ride_id = ride_timestamps.ride_id
                            WHERE event = 'dropped_off_at'
                            )
                    SELECT driver_ids.driver_id,
                    ROUND(MAX(JULIANDAY(x.timestamp) - JULIANDAY(x.offsetDate))) AS daysBetween, x.timestamp
                    FROM driver_ids
                    LEFT JOIN x ON
                    x.driver_id = driver_ids.driver_id
                    GROUP BY driver_ids.driver_id;
                """
        db.execute(query)

        return pd.DataFrame(db.fetchall(), columns=['driver_id', 'max_consecutive_offline', 'timestamp'])

    def get_days_since_last_ride(self):

        """
        Function that returns number of days since driver last drove
        """

        last_ride_in_db = self.rides["dropped_off_at"].max()
        drivers = self._get_first_last_trip()
        drivers["last_online"] = (last_ride_in_db - drivers["last_ride"]).dt.days

        return drivers

    def _get_churn(self, threshold=14):

        """
        Function that returns a Dataframe with driver_id, driver_onboard_date,
        last_ride, and is_churncolumns

        Parameters:
            threshold -> int: A driver is considered to have churned if their last ride
            is more than the given threshold (days) before last recorded ride
            in kiwi's database**
        """

        last_trip = self._get_first_last_trip()
        last_timestamp_in_kiwi_database = self.rides["dropped_off_at"].max()

        last_trip["is_churn"] = last_trip["last_ride"].\
        apply(lambda x: 1 if (last_timestamp_in_kiwi_database - x).days >= threshold else 0)



        return last_trip[["driver_id", "driver_onboard_date", "first_ride", "last_ride", "is_churn"]]



    def get_number_of_rides(self):

        """
        Function that returns a Dataframe of driver ids and total number of rides
        they did
        """

        rides = self.rides.copy()
        rides = rides.merge(self.matching_table, on="ride_id", how="left").\
            merge(self.data["drivers"], on="driver_id", how="right")[["driver_id", "ride_id"]]
        rides = rides.groupby("driver_id", as_index=False).count()
        rides.columns = ["driver_id", "ride_count"]

        return rides

    def get_total_distance(self):

        """
        Function that returns a Dataframe of driver ids and total kilometers driven
        """

        rides = self.rides.copy()
        rides = rides.merge(self.matching_table, on="ride_id", how="left").\
            merge(self.data["drivers"], on="driver_id", how="right")[["driver_id", "ride_distance"]]
        rides = rides.groupby("driver_id", as_index=False).sum()
        rides.columns = ["driver_id", "total_distance"]

        # convert to km
        rides["total_distance"] = round(rides["total_distance"]/1000, 2)
        return rides

    def get_total_hours(self):

        """
        Function that returns a Dataframe of driver ids and total hours driven
        """

        rides = self.rides.copy()
        rides = rides.merge(self.matching_table, on="ride_id", how="left").\
            merge(self.data["drivers"], on="driver_id", how="right")[["driver_id", "ride_duration_minutes"]]
        rides = rides.groupby("driver_id", as_index=False).sum()
        rides.columns = ["driver_id", "total_driving_time"]

        # convert to hours
        rides["total_driving_time"] = round(rides["total_driving_time"]/60, 2)
        return rides

    def get_total_earned(self):
        """
        Function that returns a Dataframe of driver ids and total money earned
        in US Dollars
        """

        rides = self.rides.copy()
        rides = rides.merge(self.matching_table, on="ride_id", how="left").\
            merge(self.data["drivers"], on="driver_id", how="right")[["driver_id", "fare"]]
        rides = rides.groupby("driver_id", as_index=False).sum()
        rides.columns = ["driver_id", "total_earned"]

        # driver earns 80% of total fare
        rides["total_earned"] = round(rides["total_earned"]*0.8, 2)
        return rides

    def get_primetime_rides(self):

        """
        Function that returns a Dataframe of driver ids and number of primetime
        rides
        """

        rides = self.rides.copy()
        rides = rides.merge(self.matching_table, on="ride_id", how="left").\
            merge(self.data["drivers"], on="driver_id", how="right")[["driver_id", "is_prime_time"]]
        rides = rides.groupby("driver_id", as_index=False).sum()
        rides.columns = ["driver_id", "prime_time_rides"]

        return rides

    def get_rides_first_14_days(self):
        """
        function to count number of rides within first 14 days after onboarding
        """
        rides = self.rides.copy()
        rides = rides.merge(self.matching_table, on="ride_id", how="left")[["ride_id", "driver_id", "dropped_off_at"]]

        first_rides = rides.sort_values(by=["driver_id", "dropped_off_at"], ascending=True)
        first_rides = first_rides.groupby(["driver_id", first_rides["dropped_off_at"].dt.date]).count()[["ride_id"]].reset_index()
        first_rides['cumulative_rides']=first_rides.groupby('driver_id')['ride_id'].cumsum()
        first_rides = first_rides.merge(self._get_first_last_trip(), on="driver_id", how="right")[["driver_id", "driver_onboard_date", "dropped_off_at", "cumulative_rides"]]
        first_rides["days_since_onboarding"] = (pd.to_datetime(first_rides["dropped_off_at"]) - first_rides["driver_onboard_date"]).dt.days
        first_14_days = first_rides[first_rides["days_since_onboarding"] <= 14].sort_values(["driver_id", "dropped_off_at"], ascending=False).drop_duplicates("driver_id",keep="first")
        first_14_days = first_14_days[["driver_id", "cumulative_rides"]]
        first_14_days = first_14_days.merge(self._get_first_last_trip(), on="driver_id", how="right")[["driver_id", "cumulative_rides"]].fillna(0)

        first_14_days.columns = ["driver_id", "rides_first_14_days"]

        return first_14_days

    def get_average_speed(self):
        """
        Function that returns a Dataframe of driver ids and their average speed
        in kmh
        """

        rides = self.rides.copy()
        rides = rides.merge(self.matching_table, on="ride_id", how="left").\
            merge(self.data["drivers"], on="driver_id", how="right")[["driver_id", "average_speed"]]
        rides = rides.groupby("driver_id", as_index=False).mean()
        rides.columns = ["driver_id", "average_speed"]

        return rides


    def get_average_driver_waittime(self):
        """
        Function that returns a Dataframe of driver ids and their average wait time
        for customer in seconds
        """

        rides = self.rides.copy()
        rides = rides.merge(self.matching_table, on="ride_id", how="left").\
            merge(self.data["drivers"], on="driver_id", how="right")[["driver_id", "driver_wait_time"]]
        rides = rides.groupby("driver_id", as_index=False).mean()
        rides.columns = ["driver_id", "average_waittime"]

        return rides

    def get_average_response_time(self):

        """
        Function that returns a Dataframe of driver ids and their average
        response time in seconds
        """

        rides = self.rides.copy()
        rides = rides.merge(self.matching_table, on="ride_id", how="left").\
            merge(self.data["drivers"], on="driver_id", how="right")[["driver_id", "driver_response_time"]]
        rides = rides.groupby("driver_id", as_index=False).mean()
        rides.columns = ["driver_id", "average_response_time"]

        return rides

    def get_weekend_weekday_rides(self):

        """
        Function
        """

        rides = self.rides.copy()
        rides = rides.merge(self.matching_table, on="ride_id", how="left").\
             merge(self.data["drivers"], on="driver_id", how="right")[["driver_id", "picked_up_at"]]

        rides["dayofweek"] = rides["picked_up_at"].dt.day_name()

        rides_weekend = rides[rides["dayofweek"].isin(["Saturday", "Sunday"])]
        rides_weekday = rides[~rides["dayofweek"].isin(["Saturday", "Sunday"])]

        rides_weekday = rides_weekday.groupby("driver_id", as_index=False).count()
        rides_weekend = rides_weekend.groupby("driver_id", as_index=False).count()

        rides_weekend_weekday = rides_weekday.merge(rides_weekend, on="driver_id", how="outer")[["driver_id", "dayofweek_x", "dayofweek_y"]]

        rides_weekend_weekday.columns = ["driver_id", "rides_weekday", "rides_weekend"]

        return rides_weekend_weekday.fillna(0)

    def get_lifetime_value(self, granularity=""):

        """
        Returns total average lifetime value in dollars per driver as well as dataset with
        every driver's average lifetime value
        """

        lifetime = self.get_lifetime()
        rides = self.rides.merge(self.matching_table, on="ride_id", how="left")

        # calculate kiwi revenue per ride per driver
        rides["kiwi_revenue"] = round(rides["fare"]*0.2, 2)

        # get average monthly revenue per driver
        rides = rides.groupby(["driver_id", rides["dropped_off_at"].dt.month]).sum().reset_index()[["driver_id", "dropped_off_at","kiwi_revenue"]]
        rides_average_monthly = rides.groupby("driver_id").mean().reset_index()[["driver_id","kiwi_revenue"]]
        lifetime = lifetime.merge(rides_average_monthly, on="driver_id", how="left")[["driver_id", "lifetime", "kiwi_revenue"]]

        lifetime["average_lifetime_value"] = (lifetime["lifetime"]/30) * lifetime["kiwi_revenue"]

        # rename columns
        lifetime.columns = ["driver_id", "lifetime_in_days", "kiwi_average_monthly_revenue", "average_lifetime_value"]

        # average lifetime of kiwi drivers in months
        average_lifetime = (lifetime["lifetime_in_days"]/30).mean()
        average_monthly_revenue_per_driver = rides["kiwi_revenue"].mean()

        # calculate ALTV
        average_ltv = (average_lifetime * average_monthly_revenue_per_driver)
        return (average_ltv, lifetime)

    def get_driver_training_data(self):

        """
        Returns a DataFrame with the all following columns:
        ['driver_id', 'driver_onboard_date', 'first_ride', 'last_ride',
        'is_churn', 'lifetime', 'max_consecutive_offline', 'timestamp',
        'last_online', 'ride_count', 'total_distance', 'total_driving_time',
        'total_earned', 'prime_time_rides', 'average_speed', 'average_waittime',
        'average_response_time', 'rides_weekday', 'rides_weekend',
        'lifetime_in_days', 'kiwi_average_monthly_revenue',
        'average_lifetime_value']

        """

        full_data =\
                self.get_lifetime()\
                    .merge(
                    self.get_days_between_rides(), on='driver_id', suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_days_since_last_ride(), on='driver_id', suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_number_of_rides(), on='driver_id', suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_total_distance(), on='driver_id', how="left", suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_total_hours(), on='driver_id', how="left", suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_total_earned(), on='driver_id', how="left", suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_primetime_rides(), on='driver_id', how="left", suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_average_speed(), on='driver_id', how="left", suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_average_driver_waittime(), on='driver_id', how="left", suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_average_response_time(), on='driver_id', how="left", suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_weekend_weekday_rides(), on='driver_id', how="left", suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)").merge(
                    self.get_lifetime_value()[1], on='driver_id', how="left", suffixes=('', '_DROP')
                ).filter(regex="^(?!.*DROP)")

        return full_data[['driver_id', 'driver_onboard_date', 'first_ride', 'last_ride',
        'is_churn', 'max_consecutive_offline',
        'last_online', 'ride_count', 'total_distance', 'total_driving_time',
        'total_earned', 'prime_time_rides', 'average_speed', 'average_waittime',
        'average_response_time', 'rides_weekday', 'rides_weekend',
        'lifetime_in_days', 'kiwi_average_monthly_revenue',
        'average_lifetime_value']]
