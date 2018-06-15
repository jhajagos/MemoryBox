/* SQLITE Schema for test database */

create table encounters (encounter_number int,
  pseudo_mrn varchar(10),
  admit_date_time datetime,
  discharge_date_time datetime,
  admit_type varchar(64),
  admit_source varchar(64),
  updated_at datetime
  );


create table encounter_dx (encounter_number int,
  diagnosis_type varchar(64),
  sequence_id int,
  diagnosis_code varchar(12)
);


create table encounter_procedure (encounter_number int,
  procedure varchar(64),
  sequence_id int,
  procedure_code varchar(12)
);


create table encounter_documents (encounter_number int,
  document_type varchar(64),
  document_date datetime,
  content text
);


create table patient (pseudo_mrn varchar(10),
  gender varchar(10),
  date_of_birth datetime
);
