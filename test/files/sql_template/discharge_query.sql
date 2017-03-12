select encounter_number as transaction_id from encounters where
  discharge_date_time is not null
    and discharge_date_time >= :lower_discharge_date_time and discharge_date_time < :upper_discharge_date_time