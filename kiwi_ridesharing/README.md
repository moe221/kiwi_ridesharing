## Kiwi Classes

This folder contains Kiwi Classes that handle the logic of data cleaning for this project.


### Data

```python
from kiwi_ridesharing.data import Kiwi
```

Main methods:

- `get_data`: returns all original Kiwi datasets as DataFrames within a Python dict.

### Ride

```python
from kiwi_ridesharing.data import Ride
```

Main method:
- `get_full_rides_data`: returns a DataFrame with:
  - 'ride_id'
  - 'requested_at'
  - 'accepted_at'
  - 'arrived_at'
  - 'picked_up_at'
  - 'dropped_off_at'
  - 'ride_duration_minutes'
  - 'ride_duration_hours'
  - 'average_speed'
  - 'driver_wait_time'
  - 'customer_wait_time'


### Driver

```python
from kiwi_ridesharing.data import Driver
```

Main method:
- `get_driver_training_data`: returns a DataFrame with:
  - 'driver_id',
  - 'driver_onboard_date',
  - 'first_ride',
  - 'last_ride',
  - 'is_churn',
  - 'lifetime',
  - 'max_consecutive_offline',
  - 'timestamp',
  - 'last_online',
  - 'ride_count',
  - 'total_distance',
  - 'total_driving_time',
  - 'total_earned',
  - 'prime_time_rides', 'average_speed',
  - 'average_waittime',
  - 'average_response_time',
  - 'rides_weekday',
  - 'rides_weekend',
  - 'lifetime_in_days',
  - 'kiwi_average_monthly_revenue',
  - 'average_lifetime_value'

### Utils

Utility functions to help during the project.
