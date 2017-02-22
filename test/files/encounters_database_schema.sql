/* SQLITE Schema for test database */

create table encounters (encounter_number int);

create table encounter_dx (encounter_number int);

create table encounter_documenter (encounter_number int);

create table patient (pseudo_mrn varchar(10));
