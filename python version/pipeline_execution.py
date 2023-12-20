# IMPORT PIPELINE
from pipeline import *


# READ AND CLEAN UVA HEALTH DATA

# LL_DO_NOT_DELETE_REQUIRED_concept_sets_confirmed
LL_DO_NOT_DELETE_REQUIRED_concept_sets_confirmed = pd.read_csv('LL_DO_NOT_DELETE_REQUIRED_concept_sets_confirmed.csv')

# LL_concept_sets_fusion
LL_concept_sets_fusion = pd.read_csv('New_Concepts_Table.csv')

# concept_set_members
concept_set_members = pd.read_csv('concept_set_members.csv')

# manifest
manifest = pd.read_csv('manifest.csv')

# person 
person = pd.read_csv('N3C_casecohort_person.csv')
# convert all columns to lowercase
person.columns= person.columns.str.lower()
# creating data_partner_id column with value of 1 for UVA Health for all patients
person['data_partner_id'] = 1

# measurement
# read in all measurement files
N3C_casecohort_measurement_mod1 = pd.read_csv('N3C_casecohort_measurement_mod1.csv')
N3C_casecohort_measurement_mod2 = pd.read_csv('N3C_casecohort_measurement_mod2.csv')
N3C_casecohort_measurement_mod3 = pd.read_csv('N3C_casecohort_measurement_mod3.csv')
N3C_casecohort_measurement_mod4 = pd.read_csv('N3C_casecohort_measurement_mod4.csv')
N3C_casecohort_measurement_mod5 = pd.read_csv('N3C_casecohort_measurement_mod5.csv')
N3C_casecohort_measurement_mod6 = pd.read_csv('N3C_casecohort_measurement_mod6.csv')
N3C_casecohort_measurement_mod7 = pd.read_csv('N3C_casecohort_measurement_mod7.csv')

# combine all measurement files 
measurement = pd.concat([N3C_casecohort_measurement_mod1, N3C_casecohort_measurement_mod2, 
                         N3C_casecohort_measurement_mod3, N3C_casecohort_measurement_mod4, N3C_casecohort_measurement_mod5, 
                         N3C_casecohort_measurement_mod6, N3C_casecohort_measurement_mod7])

measurement.columns= measurement.columns.str.lower()

# condition_occurrence
condition_occurrence = pd.read_csv('N3C_casecohort_condition_occurrence_mod.csv')
condition_occurrence.columns= condition_occurrence.columns.str.lower()

# visit_occurrence
visit_occurrence = pd.read_csv('N3C_casecohort_visit_occurrence_mod.csv')
visit_occurrence.columns= visit_occurrence.columns.str.lower()

# location
location = pd.read_csv('N3C_casecohort_location.csv')
location.columns= location.columns.str.lower()

# observation
observation = pd.read_csv('N3C_casecohort_observation_mod.csv')
observation.columns= observation.columns.str.lower()

# procedure_occurrence
procedure_occurrence = pd.read_csv('N3C_casecohort_procedure_occurrence_mod.csv')
procedure_occurrence.columns= procedure_occurrence.columns.str.lower()

# device_exposure
device_exposure = pd.read_csv('N3C_casecohort_device_exposure_mod.csv')
device_exposure.columns= device_exposure.columns.str.lower()

# drug_exposure
drug_exposure = pd.read_csv('N3C_casecohort_drug_exposure_mod.csv')
drug_exposure.columns= drug_exposure.columns.str.lower()

# death
death = pd.read_csv('N3C_casecohort_death_mod.csv')
death.columns= death.columns.str.lower()


# RUN TABLE FUNCTIONS ON UVA HEALTH DATA
# customize_concept_sets
customize_concept_sets = customize_concept_sets(LL_concept_sets_fusion, LL_DO_NOT_DELETE_REQUIRED_concept_sets_confirmed)

# COHORT
COHORT = COHORT(measurement, concept_set_members, person, location, manifest, condition_occurrence, visit_occurrence)

# conditions_of_interest
conditions_of_interest = conditions_of_interest(COHORT, concept_set_members, condition_occurrence, customize_concept_sets)

# observations_of_interest
observations_of_interest = observations_of_interest(observation, concept_set_members, COHORT, customize_concept_sets)

# procedures_of_interest
procedures_of_interest = procedures_of_interest(COHORT, concept_set_members, procedure_occurrence, customize_concept_sets)

# devices_of_interest
devices_of_interest = devices_of_interest(device_exposure, COHORT, concept_set_members, customize_concept_sets)

# drugs_of_interest
drugs_of_interest = drugs_of_interest(concept_set_members, drug_exposure, COHORT, customize_concept_sets)

# measurements_of_interest
measurements_of_interest = measurements_of_interest(measurement, concept_set_members, COHORT)

# visits_of_interest
visits_of_interest = visits_of_interest(COHORT, visit_occurrence, concept_set_members)

# COVID_deaths
COVID_deaths = COVID_deaths(death, COHORT, visit_occurrence, concept_set_members)

# cohort_all_facts_table
cohort_all_facts_table = cohort_all_facts_table(conditions_of_interest, measurements_of_interest, visits_of_interest, 
                                                procedures_of_interest, observations_of_interest, drugs_of_interest, 
                                                COVID_deaths, COHORT, devices_of_interest, visit_occurrence)

# COVID_Patient_Summary_Table_LDS
COVID_Patient_Summary_Table_LDS = COVID_Patient_Summary_Table_LDS(cohort_all_facts_table, COHORT, visits_of_interest, COVID_deaths, customize_concept_sets)

# VIEW FIRST FIVE ENTRIES OF SUMMARY TABLE
COVID_Patient_Summary_Table_LDS.head()