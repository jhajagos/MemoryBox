select * from patient where pseduo_mrn in
  (select pseudo_mrn from encounter e where encounter_number = :encounter_number)