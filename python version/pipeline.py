# GLOBAL IMPORTS 
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import datetime


# PIPELINE FUNCTIONS - TABLES CREATION

# customize_concept_sets

def customize_concept_sets(LL_concept_sets_fusion, LL_DO_NOT_DELETE_REQUIRED_concept_sets_confirmed):
    """
    customize_concept_sets:
        desc: combines the two fusion inputs
        ext: py
        inputs:
            - LL_concept_sets_fusion
            - LL_DO_NOT_DELETE_REQUIRED_concept_sets_confirmed
     

    Description - The purpose of this node is to optimize the user's experience connecting a customized concept set "fusion sheet" input data frame to replace LL_concept_sets_fusion_SNOMED.
    Last Update - 3/28/23
    """

    required = LL_DO_NOT_DELETE_REQUIRED_concept_sets_confirmed
    customizable = LL_concept_sets_fusion
    
    df = required.merge(customizable, on = list(required.columns), how = 'outer')
    
    return df


# COHORT

def COHORT(measurement, concept_set_members, person, location, manifest, condition_occurrence, visit_occurrence):
    """
    COHORT:
        desc: identifies cohort of patients
        ext: py
        inputs:
            - measurement
            - concept_set_members
            - person
            - location
            - manifest
            - condition_occurrence
            - visit_occurrence ## Change for UVA Health Data: visit_occurrence replaces micro_to_macrovisits table
     

    Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for the confirmed (positive COVID PCR or AG lab test or U07.1 diagnosis charted) COVID positive patients in the UVA Health data. More information can be found in the README linked here (https://unite.nih.gov/workspace/report/ri.report.main.report.51a0ea9e-e6a9-49bc-8f17-0bf357338ece).
    Creator/Owner/contact - Andrea Zhou
    Last Update - 3/28/23
    Description - This node identifies all patients with positive results from a PCR or AG COVID-19 lab test and the date of the patients' first instance of this type of COVID-19+ test.  It also identifies all patients with a COVID-19 diagnosis charted and the date of the patients’ first instance of this type of diagnosis (when available).  The earlier of the two is considered the index date for downstream calculations.  This transform then gathers some commonly used facts about these patients from the "person" and "location" tables, as well as some facts about the patient's institution (from the "manifest" table).  Available age, race, ethnicity, and locations data is gathered at this node.  The patient’s number of visits before and after covid as well as the number of days in their observation period before and after covid is calculated from the “visit_occurrence” table in this node.  These facts will eventually be joined with the final patient-level table in the final node. Embedded in the function, select proportion of enclave patients to use: A value of 1.0 indicates the pipeline will use all patients in the persons table.  A value less than 1.0 takes a random sample of the patients with a value of 0.001 (for example) representing a 0.1% sample of the persons table will be used.
    """
    proportion_of_patients_to_use = 1.0
    concepts_df = concept_set_members
    
    ## Change for UVA Health Data: day_of_birth removed from column selection
    person_sample = person[['person_id','year_of_birth','month_of_birth','gender_source_value',
                            'location_id','data_partner_id', 
                           'race_source_value']]
    person_sample.rename(columns={'gender_source_value':'sex'},inplace=True)
    person_sample = person_sample.drop_duplicates()
    person_sample = person_sample.sample(int(proportion_of_patients_to_use*len(person_sample)))

    measurement_df = measurement[['person_id', 'measurement_date', 'measurement_concept_id', 'value_as_concept_id']]
    measurement_df = measurement_df.loc[measurement_df['measurement_date'].notnull()]
    measurement_df =  pd.merge(measurement_df,person_sample, on='person_id', how='inner')

    conditions_df = condition_occurrence[['person_id', 'condition_start_date', 'condition_concept_id']]
    conditions_df = conditions_df.loc[conditions_df['condition_start_date'].notnull()]
    conditions_df = pd.merge(conditions_df,person_sample,on='person_id', how='inner')
    
    visits_df = visit_occurrence[["person_id", "visit_start_date"]]
    
    manifest_df = manifest[['data_partner_id','run_date','cdm_name','cdm_version','shift_date_yn','max_num_shift_days']]
    
    manifest_df.rename(columns={"run_date":"data_extraction_date"},inplace=True)
    
    location_df = location.drop_duplicates(subset=['location_id'])
    location_df = location_df[['location_id','city','state','zip','county']]
    location_df.rename(columns={'zip':'postal_code'},inplace=True)
    
    # make list of concept IDs for Covid tests and filter measurements table for only these concept IDs.  Then make list of concept IDs for POSITIVE Covid tests and label covid test measurements table as 1 for pos covid tests concept IDs and 0 for not
    
    covid_measurement_test_ids = list(concepts_df.loc[((concepts_df.concept_set_name=="ATLAS SARS-CoV-2 rt-PCR and AG")&
                                                      (concepts_df.is_most_recent_version=='t'))]['concept_id'])
    
    covid_positive_measurement_ids = list(concepts_df.loc[((concepts_df.concept_set_name=="ResultPos")&
                                                      (concepts_df.is_most_recent_version=='t'))]['concept_id'])

    measurements_of_interest = measurement_df.loc[measurement_df.measurement_concept_id.isin(covid_measurement_test_ids)]
    
    measurements_of_interest = measurements_of_interest.loc[measurements_of_interest.value_as_concept_id.isin(covid_positive_measurement_ids)]

    measurements_of_interest = measurements_of_interest.rename(columns={"measurement_date":"covid_measurement_date"}).drop_duplicates(subset=['person_id','covid_measurement_date'])
    measurements_of_interest = measurements_of_interest[['person_id','covid_measurement_date']]
    
    first_covid_pos_lab = measurements_of_interest.groupby('person_id').agg({'covid_measurement_date':['min']})
    first_covid_pos_lab.columns = ['COVID_first_PCR_or_AG_lab_positive']
    first_covid_pos_lab = first_covid_pos_lab.reset_index()
    
    # add flag for first date of COVID-19 diagnosis code if available
    COVID_concept_ids = list(concepts_df.loc[((concepts_df.concept_set_name=="N3C Covid Diagnosis")&
                                                      (concepts_df.is_most_recent_version=='t'))]['concept_id'])

    conditions_of_interest = conditions_df.loc[conditions_df.condition_concept_id.isin(COVID_concept_ids)]
    conditions_of_interest = conditions_of_interest.rename(columns={"condition_start_date":"covid_DIAGNOSIS_date"}).drop_duplicates(subset=['person_id','covid_DIAGNOSIS_date'])
    conditions_of_interest = conditions_of_interest[['person_id','covid_DIAGNOSIS_date']]

    first_covid_DIAGNOSIS = conditions_of_interest.groupby('person_id').agg({'covid_DIAGNOSIS_date':['min']})
    first_covid_DIAGNOSIS.columns = ['COVID_first_diagnosis_date']
    first_covid_DIAGNOSIS = first_covid_DIAGNOSIS.reset_index()

    #join lab positive with diagnosis positive to create all confirmed covid patients cohort
    df = pd.merge(first_covid_pos_lab, first_covid_DIAGNOSIS, on='person_id', how='outer')
    
    #add a column for the earlier of the diagnosis or the lab test dates for all confirmed covid patients
    df['COVID_first_PCR_or_AG_lab_positive'] = pd.to_datetime(df['COVID_first_PCR_or_AG_lab_positive'])
    df['COVID_first_diagnosis_date'] = pd.to_datetime(df['COVID_first_diagnosis_date'])
    df["COVID_first_poslab_or_diagnosis_date"] = df[['COVID_first_PCR_or_AG_lab_positive','COVID_first_diagnosis_date']].min(axis=1)
    
    #add in demographics+locations data for all confirmed covid patients
    df = pd.merge(df, person_sample, on='person_id',how='inner')
    #join in location_df data for all confirmed covid patients
    df = pd.merge(df, location_df, on='location_id', how='left')

    #join in manifest_df information
    df = pd.merge(df,manifest_df,on='data_partner_id',how='inner')
    df['max_num_shift_days'] = df['max_num_shift_days'].replace("",'0').replace('na','0')
    
    #calculate date of birth for all confirmed covid patients
    df['new_year_of_birth'] = df['year_of_birth'].fillna(1)
    df['new_month_of_birth'] = df['month_of_birth'].fillna(7).replace(0, 7)
    
    ## Change for UVA Health Data: Data has no day_of_birth column for reference so assign new_day_of_birth column to default value of 1
    df['new_day_of_birth'] =  1
    
    df['date_of_birth'] = pd.to_datetime(dict(year=df['new_year_of_birth'], 
                                              month=df['new_month_of_birth'], day=df['new_day_of_birth']))
    df['date_of_birth'] = df['date_of_birth'].dt.date
    
    #convert date of birth string to date and apply min and max reasonable birthdate filter parameters, inclusive
    max_shift_as_int = df
    max_shift_as_int["shift_days_as_int"] = max_shift_as_int['max_num_shift_days'].astype('int').max()
    min_reasonable_dob = datetime.date(1902, 1, 1) 
    max_reasonable_dob = datetime.date.today() + datetime.timedelta(days=int(max_shift_as_int['max_num_shift_days'][0]))

    df["date_of_birth"] = np.where(df['date_of_birth'].between(min_reasonable_dob, max_reasonable_dob),df['date_of_birth'],None)
    
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"]).dt.date
    df["COVID_first_poslab_or_diagnosis_date"] = pd.to_datetime(df["COVID_first_poslab_or_diagnosis_date"]).dt.date 
    
    m = df[['COVID_first_poslab_or_diagnosis_date','date_of_birth']].notnull().all(axis=1)
    df.loc[m, 'age_at_covid'] = df[m].apply(lambda x: relativedelta(x['COVID_first_poslab_or_diagnosis_date'], x['date_of_birth']).years, axis=1)
    
    H = ['Hispanic']
    H2 = ['Hispanic', 'Hispanic or Latino']
    A = ['Asian', 'Asian Indian', 'Bangladeshi', 'Bhutanese', 'Burmese', 'Cambodian', 'Chinese', 'Filipino', 'Hmong', 'Indonesian', 'Japanese', 'Korean', 'Laotian', 'Malaysian', 'Maldivian', 'Nepalese', 'Okinawan', 'Pakistani', 'Singaporean', 'Sri Lankan', 'Taiwanese', 'Thai', 'Vietnamese']
    B_AA = ['African', 'African American', 'Barbadian', 'Black', 'Black or African American', 'Dominica Islander', 'Haitian', 'Jamaican', 'Madagascar', 'Trinidadian', 'West Indian']
    W = ['White','White or Caucasian']
    NH_PI = ['Melanesian', 'Micronesian', 'Native Hawaiian or Other Pacific Islander', 'Other Pacific Islander', 'Polynesian', 'Native Hawaiian and Other Pacific Islander']
    AI_AN = ['American Indian or Alaska Native', 'American Indian and Alaska Native']
    O = ['More than one race', 'Multiple race', 'Multiple races', 'Other', 'Other Race']
    U = ['Asian or Pacific Islander', 'No Information', 'No matching concept', 'Refuse to Answer', 'Unknown', 'Unknown racial group', 'Patient Unavailable', 'Patient Refused', 'Unavailable']
    
    conditions = [
        (df["race_source_value"].isin(H)),
        (df["race_source_value"].isin(A)),
        (df["race_source_value"].isin(B_AA)),
        (df["race_source_value"].isin(W)),
        (df["race_source_value"].isin(NH_PI)),
        (df["race_source_value"].isin(AI_AN)),
        (df["race_source_value"].isin(O)),
        (df["race_source_value"].isin(U)),]
       
    choices = ["Hispanic or Latino",
               "Asian",
               "Black or African American",
               "White",
               "Native Hawaiian or Other Pacific Islander",
               "American Indian or Alaska Native",
               "Other",
               "Unknown"]
    
    df["race"] = np.select(conditions, choices, default="Unknown")
    
    conditions_ethnicity = [
        (df["race_source_value"].isin(H2)),
        (df["race_source_value"].isin(A)),
        (df["race_source_value"].isin(B_AA)),
        (df["race_source_value"].isin(W)),
        (df["race_source_value"].isin(NH_PI)),
        (df["race_source_value"].isin(AI_AN)),
        (df["race_source_value"].isin(O)),
        (df["race_source_value"].isin(U)),
    ]
    
    choices_ethnicity = ["Hispanic or Latino Any Race",
                         "Asian Non-Hispanic",
                         "Black or African American Non-Hispanic",
                         "White Non-Hispanic",
                         "Native Hawaiian or Other Pacific Islander Non-Hispanic",
                         "American Indian or Alaska Native Non-Hispanic",
                         "Other Non-Hispanic",
                         "Unknown"]

    df["race_ethnicity"] = np.select(conditions_ethnicity, choices_ethnicity, default="Unknown")
    
    #create visit counts/obs period for before and post COVID
    hosp_visits = visits_df.loc[visits_df["visit_start_date"].notnull()]
    hosp_visits = hosp_visits.sort_values(by=["visit_start_date"]).drop_duplicates(subset=["person_id", "visit_start_date"])
    
    non_hosp_visits = visits_df.loc[visits_df["visit_start_date"].isnull()]
    non_hosp_visits = non_hosp_visits.drop_duplicates(subset=["person_id", "visit_start_date"])
    visits_df = pd.concat([hosp_visits,non_hosp_visits]).reset_index() #join the two
   
    #join in earliest index date value and use to calculate datediff between lab and visit. If positive then date is before the PCR/AG+ date. If negative then date is after the PCR/AG+ date.
    
    visits_df = visits_df.merge(df[['person_id','COVID_first_poslab_or_diagnosis_date','shift_date_yn',
                                    'max_num_shift_days']], on='person_id', how='inner')
    
    visits_df["COVID_first_poslab_or_diagnosis_date"] = pd.to_datetime(visits_df["COVID_first_poslab_or_diagnosis_date"]).dt.date 
    visits_df["visit_start_date"] = pd.to_datetime(visits_df["visit_start_date"]).dt.date 
    
    
    visits_df['earliest_index_minus_visit_start_date'] = (visits_df['COVID_first_poslab_or_diagnosis_date'] - 
                                                          visits_df['visit_start_date']).dt.days
    
    #counts for visits before
    visits_before = visits_df[visits_df['earliest_index_minus_visit_start_date']>0].groupby(['person_id']).size().reset_index(name='number_of_visits_before_covid')
    
    #obs period in days before, where earliest_index_minus_visit_start_date = 0 means the pt_max_visit_date is the index date
    observation_before = visits_df[visits_df['earliest_index_minus_visit_start_date'] >= 0].groupby(['person_id']).agg({'visit_start_date': ['max','min']})
    observation_before.columns = ['pt_max_visit_date','pt_min_visit_date']
    observation_before = observation_before.reset_index()
    observation_before['observation_period_before_covid'] = (observation_before['pt_max_visit_date']-
                                                             observation_before['pt_min_visit_date']).dt.days
    observation_before = observation_before[['person_id', 'observation_period_before_covid']]

    visits_post = visits_df[visits_df['earliest_index_minus_visit_start_date']<0].groupby(['person_id']).size().reset_index(name='number_of_visits_post_covid')

    #obs period in days after, where earliest_index_minus_visit_start_date = 0 means the pt_min_visit_date is the index date
    observation_post = visits_df[visits_df['earliest_index_minus_visit_start_date'] <= 0].groupby(['person_id']).agg({'visit_start_date': ['max','min']})
    observation_post.columns = ['pt_max_visit_date','pt_min_visit_date']
    observation_post = observation_post.reset_index()
    observation_post['observation_period_post_covid'] = (observation_post['pt_max_visit_date']-
                                                           observation_post['pt_min_visit_date']).dt.days
    observation_post = observation_post[['person_id', 'observation_period_post_covid']]

    #join visit counts/obs periods dataframes with main dataframe
    df = pd.merge(df, visits_before, on="person_id", how="left")
    df = pd.merge(df, observation_before, on="person_id", how="left")
    df = pd.merge(df, visits_post, on="person_id", how="left")
    df = pd.merge(df, observation_post, on="person_id", how="left")

    #LEVEL 2 ONLY
    df = df[['person_id',
            'COVID_first_PCR_or_AG_lab_positive',
            'COVID_first_diagnosis_date',
            'COVID_first_poslab_or_diagnosis_date',
            'number_of_visits_before_covid',
            'observation_period_before_covid',
            'number_of_visits_post_covid',
            'observation_period_post_covid',
            'sex',
            'city',
            'state',
            'postal_code',
            'county',
            'age_at_covid',
            'race',
            'race_ethnicity',
            'data_partner_id',
            'data_extraction_date',
            'cdm_name',
            'cdm_version',
            'shift_date_yn',
            'max_num_shift_days']]
        
    return df


# conditions_of_interest

def conditions_of_interest(COHORT, concept_set_members, condition_occurrence, customize_concept_sets):
    """
    conditions_of_interest:
        desc: finds condition concepts
        ext: py
        inputs:
            - COHORT
            - concept_set_members
            - condition_occurrence
            - customize_concept_sets
 

    Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for the confirmed (positive COVID PCR or AG lab test or U07.1 diagnosis charted) COVID positive patients in the UVA Health data.
    Creator/Owner/contact - Andrea Zhou
    Last Update - 3/28/23
    Description - This node filters the condition_occurence table for rows that have a condition_concept_id associated with one of the conceptsets described in the data dictionary in the README through the use of a fusion sheet.  Indicator names for these conditions are assigned, and the indicators are collapsed to unique instances on the basis of patient and date.
    """

    #bring in only cohort patient ids
    persons = COHORT['person_id']
    
    #filter observations table to only cohort patients
    conditions_df = condition_occurrence[['person_id', 'condition_start_date', 'condition_concept_id']]
    conditions_df = conditions_df[conditions_df['condition_start_date'].notna()]
    conditions_df.rename(columns={'condition_start_date': 'date', 
                               'condition_concept_id': 'concept_id'}, inplace=True)
    conditions_df = conditions_df.merge(persons, on='person_id', how='inner')

    #filter fusion sheet for concept sets and their future variable names that have concepts in the conditions domain
    fusion_df = customize_concept_sets[customize_concept_sets['domain'].str.contains('condition')][['concept_set_name','indicator_prefix']]
    
    #filter concept set members table to only concept ids for the conditions of interest
    concepts_df = concept_set_members[['concept_set_name', 'is_most_recent_version', 'concept_id']]
    concepts_df = concepts_df.loc[concepts_df['is_most_recent_version'] == 't']
    concepts_df = concepts_df.merge(fusion_df, on = 'concept_set_name', how = 'inner')
    concepts_df = concepts_df[['concept_id','indicator_prefix']]

    #find conditions information based on matching concept ids for conditions of interest
    df = conditions_df.merge(concepts_df, on = 'concept_id', how = 'inner')
    
    #collapse to unique person and visit date and pivot on future variable name to create flag for rows associated with the concept sets for conditions of interest    
    df = df[['person_id','date']].join(pd.get_dummies(df['indicator_prefix'])).groupby(['person_id',
                                                                            'date']).max().reset_index()
   
    return df


# observations_of_interest

def observations_of_interest(observation, concept_set_members, COHORT, customize_concept_sets):
    """
    observations_of_interest:
        desc: finds observation concepts
        ext: py
        inputs:
            - observation
            - concept_set_members
            - COHORT
            - customize_concept_sets
 

    Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for the confirmed (positive COVID PCR or AG lab test or U07.1 diagnosis charted) COVID positive patients in the UVA Health data.
    Creator/Owner/contact - Andrea Zhou
    Last Update - 3/28/23
    Description - This node filters the source OMOP tables for rows that have a standard concept id associated with one of the concept sets described in the data dictionary in the README through the use of a fusion sheet.  Indicator names for these variables are assigned, and the indicators are collapsed to unique instances on the basis of patient and date.
    """
    #bring in only cohort patient ids
    persons = COHORT['person_id']
    
    #filter observations table to only cohort patients   
    observations_df = observation[['person_id','observation_date','observation_concept_id']]
    observations_df = observations_df[observations_df['observation_date'].notna()]
    observations_df.rename(columns={'observation_date': 'date', 
                               'observation_concept_id': 'concept_id'}, inplace=True)
    observations_df = observations_df.merge(persons, on='person_id', how='inner')

    #filter fusion sheet for concept sets and their future variable names that have concepts in the observations domain
    fusion_df = customize_concept_sets[customize_concept_sets['domain'].str.contains('observation')][['concept_set_name','indicator_prefix']]
    
    #filter concept set members table to only concept ids for the observations of interest
    concepts_df = concept_set_members[['concept_set_name', 'is_most_recent_version', 'concept_id']]
    concepts_df = concepts_df.loc[concepts_df['is_most_recent_version'] == 't']
    concepts_df = concepts_df.merge(fusion_df, on = 'concept_set_name', how = 'inner')
    concepts_df = concepts_df[['concept_id','indicator_prefix']]

    #find observations information based on matching concept ids for observations of interest
    df = observations_df.merge(concepts_df, on = 'concept_id', how = 'inner')
    
    #collapse to unique person and visit date and pivot on future variable name to create flag for rows associated with the concept sets for observations of interest    
    df = df[['person_id','date']].join(pd.get_dummies(df['indicator_prefix'])).groupby(['person_id',
                                                                            'date']).max().reset_index()

    return df


# procedures_of_interest

def procedures_of_interest(COHORT, concept_set_members, procedure_occurrence, customize_concept_sets):
    """
    procedures_of_interest:
        desc: finds procedure concepts
        ext: py
        inputs:
            - COHORT
            - concept_set_members
            - procedure_occurrence
            - customize_concept_sets
 

    Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for the confirmed (positive COVID PCR or AG lab test or U07.1 diagnosis charted) COVID positive patients in the UVA Health data.
    Creator/Owner/contact - Andrea Zhou
    Last Update - 3/28/23
    Description - This node filters the source OMOP tables for rows that have a standard concept id associated with one of the concept sets described in the data dictionary in the README through the use of a fusion sheet.  Indicator names for these variables are assigned, and the indicators are collapsed to unique instances on the basis of patient and date.
    """

    #bring in only cohort patient ids
    persons = COHORT['person_id']
    
    #filter procedure occurrence table to only cohort patients   
    procedures_df = procedure_occurrence[['person_id','procedure_date','procedure_concept_id']]
    procedures_df = procedures_df[procedures_df['procedure_date'].notna()]
    procedures_df.rename(columns={'procedure_date': 'date', 
                               'procedure_concept_id': 'concept_id'}, inplace=True)
    procedures_df = procedures_df.merge(persons, on='person_id', how='inner')

    #filter fusion sheet for concept sets and their future variable names that have concepts in the procedure domain
    fusion_df = customize_concept_sets[customize_concept_sets['domain'].str.contains('procedure')][['concept_set_name','indicator_prefix']]
    
    #filter concept set members table to only concept ids for the procedures of interest
    concepts_df = concept_set_members[['concept_set_name', 'is_most_recent_version', 'concept_id']]
    concepts_df = concepts_df.loc[concepts_df['is_most_recent_version'] == 't']
    concepts_df = concepts_df.merge(fusion_df, on = 'concept_set_name', how = 'inner')
    concepts_df = concepts_df[['concept_id','indicator_prefix']]
 
    #find procedure occurrence information based on matching concept ids for procedures of interest
    df = procedures_df.merge(concepts_df, on = 'concept_id', how = 'inner')
    
    #collapse to unique person and visit date and pivot on future variable name to create flag for rows associated with the concept sets for procedures of interest    
    df = df[['person_id','date']].join(pd.get_dummies(df['indicator_prefix'])).groupby(['person_id',
                                                                            'date']).max().reset_index()

    return df


# devices_of_interest

def devices_of_interest(device_exposure, COHORT, concept_set_members, customize_concept_sets):
    """
    devices_of_interest:
        desc: finds device concepts
        ext: py
        inputs:
            - device_exposure
            - COHORT
            - concept_set_members
            - customize_concept_sets
 

    Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for the confirmed (positive COVID PCR or AG lab test or U07.1 diagnosis charted) COVID positive patients in the UVA Health data.
    Creator/Owner/contact - Andrea Zhou
    Last Update - 3/28/23
    Description - This node filters the source OMOP tables for rows that have a standard concept id associated with one of the concept sets described in the data dictionary in the README through the use of a fusion sheet.  Indicator names for these variables are assigned, and the indicators are collapsed to unique instances on the basis of patient and date.

    """

    #bring in only cohort patient ids
    persons = COHORT['person_id']
    
    #filter device exposure table to only cohort patients
    devices_df = device_exposure[['person_id','device_exposure_start_date','device_concept_id']]
    devices_df = devices_df[devices_df['device_exposure_start_date'].notna()]
    devices_df.rename(columns={'device_exposure_start_date': 'date', 
                               'device_concept_id': 'concept_id'}, inplace=True)
    devices_df = devices_df.merge(persons, on='person_id', how='inner')

    #filter fusion sheet for concept sets and their future variable names that have concepts in the devices domain
    fusion_df = customize_concept_sets[customize_concept_sets['domain'].str.contains('device')][['concept_set_name','indicator_prefix']]
    
    #filter concept set members table to only concept ids for the devices of interest
    concepts_df = concept_set_members[['concept_set_name', 'is_most_recent_version', 'concept_id']]
    concepts_df = concepts_df.loc[concepts_df['is_most_recent_version'] == 't']
    
    concepts_df = concepts_df.merge(fusion_df, on = 'concept_set_name', how = 'inner')
    concepts_df = concepts_df[['concept_id','indicator_prefix']]
        
    #find device exposure information based on matching concept ids for devices of interest
    df = devices_df.merge(concepts_df, on = 'concept_id', how = 'inner')
    
    #collapse to unique person and visit date and pivot on future variable name to create flag for rows associated with the concept sets for devices of interest
    df = df[['person_id','date']].join(pd.get_dummies(df['indicator_prefix'])).groupby(['person_id',
                                                                            'date']).max().reset_index()
    
    ## Change for UVA Health Data: Additional check for UVA analysis because current data has no devices of interest.  Allows for correct merging of table later.
    if df.empty:
        df = pd.DataFrame(columns=['person_id', 'date'])

    return df


# drugs_of_interest

def drugs_of_interest(concept_set_members, drug_exposure, COHORT, customize_concept_sets):
    """
    drugs_of_interest:
        desc: finds drug concepts
        ext: py
        inputs:
            - concept_set_members
            - drug_exposure
            - COHORT
            - customize_concept_sets
     

    Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for the confirmed (positive COVID PCR or AG lab test or U07.1 diagnosis charted) COVID positive patients in the UVA Health data.
    Creator/Owner/contact - Andrea Zhou
    Last Update - 3/28/23
    Description - This node filters the source OMOP tables for rows that have a standard concept id associated with one of the concept sets described in the data dictionary in the README through the use of a fusion sheet.  Indicator names for these variables are assigned, and the indicators are collapsed to unique instances on the basis of patient and date.
    """
  
    #bring in only cohort patient ids
    persons = COHORT['person_id']
    
    #filter drug exposure table to only cohort patients
    drug_df = drug_exposure[['person_id','drug_exposure_start_date','drug_concept_id']]
    drug_df = drug_df[drug_df['drug_exposure_start_date'].notna()]
    drug_df.rename(columns={'drug_exposure_start_date': 'date', 
                               'drug_concept_id': 'concept_id'}, inplace=True)
    drug_df = drug_df.merge(persons, on='person_id', how='inner')

    #filter fusion sheet for concept sets and their future variable names that have concepts in the drug domain
    fusion_df = customize_concept_sets[customize_concept_sets['domain'].str.contains('drug')][['concept_set_name','indicator_prefix']]
    
    #filter concept set members table to only concept ids for the drugs of interest
    concepts_df = concept_set_members[['concept_set_name', 'is_most_recent_version', 'concept_id']]
    concepts_df = concepts_df.loc[concepts_df['is_most_recent_version'] == 't']
    concepts_df = concepts_df.merge(fusion_df, on = 'concept_set_name', how = 'inner')
    concepts_df = concepts_df[['concept_id','indicator_prefix']]
        
    #find drug exposure information based on matching concept ids for drugs of interest
    df = drug_df.merge(concepts_df, on = 'concept_id', how = 'inner')
    
    #collapse to unique person and visit date and pivot on future variable name to create flag for rows associated with the concept sets for drugs of interest  
    df = df[['person_id','date']].join(pd.get_dummies(df['indicator_prefix'])).groupby(['person_id',
                                                                            'date']).max().reset_index()
    
    return df


# measurements_of_interest

def measurements_of_interest(measurement, concept_set_members, COHORT):
    """
    measurements_of_interest:
        desc: finds measurement concepts and values
        ext: py
        inputs:
            - measurement
            - concept_set_members
            - COHORT
 

    Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for the confirmed (positive COVID PCR or AG lab test or U07.1 diagnosis charted) COVID positive patients in the UVA Health data.
    Creator/Owner/contact - Andrea Zhou
    Last Update - 3/28/23
    Description - This node filters the measurements table for rows that have a measurement_concept_id associated with one of the concept sets described in the data dictionary in the README.  It finds the harmonized value as a number for the quantitative measurements and collapses these values to unique instances on the basis of patient and date.  It also finds the value as concept id for the qualitative measurements (covid labs) and collapses these to unique instances on the basis of patient and date.  Measurement BMI cutoffs included are intended for adults. Analyses focused on pediatric measurements should use different bounds for BMI measurements.

    """
    
    #bring in only cohort patient ids
    persons = COHORT['person_id']
    
    #filter procedure occurrence table to only cohort patients   
    df = measurement[['person_id','measurement_date','measurement_concept_id',
                      'value_as_number', 'value_as_concept_id']]
    df = df[df['measurement_date'].notna()]
    df.rename(columns={'measurement_date': 'date'}, inplace=True)
    df = df.merge(persons, on='person_id', how='inner')
        
    concepts_df = concept_set_members[['concept_set_name', 'is_most_recent_version', 'concept_id']]
    concepts_df = concepts_df.loc[concepts_df['is_most_recent_version'] == 't']
          
    #Find BMI closest to COVID using both reported/observed BMI and calculated BMI using height and weight.  Cutoffs for reasonable height, weight, and BMI are provided and can be changed by the template user.
    lowest_acceptable_BMI = 10
    highest_acceptable_BMI = 100
    ## Change for UVA Health Data: acceptable units converted to ounces and inches to match data units
    lowest_acceptable_weight = 176.37 #in ounces
    highest_acceptable_weight = 10582.2 #in ounces
    lowest_acceptable_height = 23.622 #in inches
    highest_acceptable_height = 95.66929 #in inches
    
    bmi_codeset_ids = list(concepts_df.loc[(concepts_df['concept_set_name'] == 'body mass index') & 
                            (concepts_df['is_most_recent_version'] == 't')]['concept_id'])
    
    weight_codeset_ids = list(concepts_df.loc[(concepts_df['concept_set_name'] == 'Body weight (LG34372-9 and SNOMED)') & 
                                              (concepts_df['is_most_recent_version'] == 't')]['concept_id'])
    
    height_codeset_ids = list(concepts_df.loc[(concepts_df['concept_set_name'] == 'Height (LG34373-7 + SNOMED)') & 
                                              (concepts_df['is_most_recent_version'] == 't')]['concept_id'])
    
    pcr_ag_test_ids = list(concepts_df.loc[(concepts_df['concept_set_name'] == 'ATLAS SARS-CoV-2 rt-PCR and AG') & 
                                           (concepts_df['is_most_recent_version'] == 't')]['concept_id'])
    
    antibody_test_ids = list(concepts_df.loc[(concepts_df['concept_set_name'] == 'Atlas #818 [N3C] CovidAntibody retry') & 
                                             (concepts_df['is_most_recent_version'] == 't')]['concept_id'])
    
    covid_positive_measurement_ids = list(concepts_df.loc[(concepts_df['concept_set_name'] == 'ResultPos') & 
                                                          (concepts_df['is_most_recent_version'] == 't')]['concept_id'])
    
    covid_negative_measurement_ids = list(concepts_df.loc[(concepts_df['concept_set_name'] == 'ResultNeg') & 
                                                          (concepts_df['is_most_recent_version'] == 't')]['concept_id'])

    #add value columns for rows associated with the above concept sets, but only include BMI or height or weight when in reasonable range
    BMI_df = df[df['value_as_number'].notna()]
    BMI_df['Recorded_BMI'] = np.where((BMI_df['measurement_concept_id'].isin(bmi_codeset_ids)) & 
                                      (BMI_df['value_as_number'].between(lowest_acceptable_BMI, highest_acceptable_BMI, inclusive='both')), 
                                      BMI_df['value_as_number'], 0)
    BMI_df['height'] = np.where((BMI_df['measurement_concept_id'].isin(height_codeset_ids)) & 
                                      (BMI_df['value_as_number'].between(lowest_acceptable_height, highest_acceptable_height, inclusive='both')), 
                                      BMI_df['value_as_number'], 0)
    BMI_df['weight'] = np.where((BMI_df['measurement_concept_id'].isin(weight_codeset_ids)) & 
                                      (BMI_df['value_as_number'].between(lowest_acceptable_weight, highest_acceptable_weight, inclusive='both')), 
                                      BMI_df['value_as_number'], 0)
    
    labs_df = df
    labs_df['PCR_AG_Pos'] = np.where((labs_df['measurement_concept_id'].isin(pcr_ag_test_ids)) & 
                                      (labs_df['value_as_concept_id'].isin(covid_positive_measurement_ids)), 
                                      1, 0)
    labs_df['PCR_AG_Neg'] = np.where((labs_df['measurement_concept_id'].isin(pcr_ag_test_ids)) & 
                                      (labs_df['value_as_concept_id'].isin(covid_negative_measurement_ids)), 
                                      1, 0)
    labs_df['Antibody_Pos'] = np.where((labs_df['measurement_concept_id'].isin(antibody_test_ids)) & 
                                      (labs_df['value_as_concept_id'].isin(covid_positive_measurement_ids)), 
                                      1, 0)
    labs_df['Antibody_Neg'] = np.where((labs_df['measurement_concept_id'].isin(antibody_test_ids)) & 
                                      (labs_df['value_as_concept_id'].isin(covid_negative_measurement_ids)), 
                                      1, 0)
     
    #collapse all reasonable values to unique person and visit rows
    BMI_df = BMI_df.groupby(['person_id', 'date']).agg({'Recorded_BMI':'max', 'height':'max', 'weight':'max'}).reset_index()
    labs_df = labs_df.groupby(['person_id', 'date']).agg({'PCR_AG_Pos':'max', 'PCR_AG_Neg':'max', 'Antibody_Pos':'max', 'Antibody_Neg':'max'})

    #add a calculated BMI for each visit date when height and weight available.  Note that if only one is available, it will result in zero
    #subsequent filter out rows that would have resulted from unreasonable calculated_BMI being used as best_BMI for the visit

    ## Change for UVA Health Data: BMI calculation converted for ounces and inches as units (formula from CDC)
    BMI_df['calculated_BMI'] = (BMI_df['weight']*0.0625)/(BMI_df['height']*BMI_df['height']) * 703
    
    BMI_df['BMI'] = np.where(BMI_df['Recorded_BMI'] > 0, BMI_df['Recorded_BMI'], BMI_df['calculated_BMI'])
    BMI_df = BMI_df[['person_id','date','BMI']]
    
    BMI_df = BMI_df[BMI_df['BMI'].between(lowest_acceptable_BMI, highest_acceptable_BMI, inclusive='both')]
    BMI_df['BMI_rounded'] = round(BMI_df['BMI']).astype('int')
    BMI_df = BMI_df.drop('BMI', axis = 1)
    
    BMI_df['OBESITY'] = np.where(BMI_df['BMI_rounded'] >= 30, 1, 0)

    #join BMI_df with labs_df to retain all lab results with only reasonable BMI_rounded and OBESITY flags
    df = labs_df.merge(BMI_df, on = ['person_id', 'date'], how = 'left')

    return df


# visits_of_interest

def visits_of_interest(COHORT, visit_occurrence, concept_set_members):
    """
    visits_of_interest:
        desc: finds ED and hosp visit concepts within desired timeframe
        ext: py
        inputs:
            - COHORT
            - visit_occurrence ## Change for UVA Health Data: visit_occurrence replaces micro_to_macrovisits table
            - concept_set_members
 

    Purpose - The purpose of this pipeline is to produce a visit day level and a persons level fact table for the confirmed (positive COVID PCR or AG lab test or U07.1 diagnosis charted) COVID positive patients in the UVA Health data.
    Creator/Owner/contact - Andrea Zhou
    Last Update - 3/28/23
    Description - This node queries the visit_occurrence table to identify hospitalizations. The parameter called covid_associated_hospitalization_requires_lab_AND_diagnosis is created and allows the user to easily change whether they define COVID-19 associated ED visits and hospitalizations using the CDC definition (lab positive with a COVID-19 diagnosis charted) OR using anyone who is either lab positive or has a COVID-19 diagnosis charted.  Number of days between a patient’s diagnosis date and their positive lab result is also calculated in this node.
    """
    
    #select test/dx date columns for cohort patients and add column for date diff between positive lab test and COVID diagnosis when available
    persons = COHORT[['person_id', 'COVID_first_PCR_or_AG_lab_positive', 'COVID_first_diagnosis_date', 'COVID_first_poslab_or_diagnosis_date']]
    
    persons['COVID_first_PCR_or_AG_lab_positive'] = pd.to_datetime(persons['COVID_first_PCR_or_AG_lab_positive'])
    persons['COVID_first_diagnosis_date'] = pd.to_datetime(persons['COVID_first_diagnosis_date'])
    persons['COVID_first_poslab_or_diagnosis_date'] = pd.to_datetime(persons['COVID_first_poslab_or_diagnosis_date'])
    
    persons['lab_minus_diagnosis_date'] = (persons['COVID_first_PCR_or_AG_lab_positive'] - persons['COVID_first_diagnosis_date']).dt.days
    
    #filter visit table to only cohort patients    
    df = visit_occurrence[['person_id','visit_start_date','visit_concept_id','visit_end_date']] # *****Customized for external to enclave execution*****
    df = df.merge(persons, on = 'person_id', how = 'inner')
    
    concepts_df = concept_set_members[['concept_set_name', 'is_most_recent_version', 'concept_id']]
    concepts_df = concepts_df.loc[concepts_df['is_most_recent_version'] == 't']

    # use visit_occurrence table to find ED only visits (that do not lead to hospitalization)  
    ## Change for UVA Health Data: below code customized for visit_occurrence table
    ED_concept_ids = list(concepts_df.loc[(concepts_df['concept_set_name'] == '[PASC] ED Visits') & 
                            (concepts_df['is_most_recent_version'] == 't')]['concept_id'])
    df_ED = df[(df['visit_start_date'].notna()) & (df['visit_concept_id'].isin(ED_concept_ids))]
    
    df_ED["COVID_first_PCR_or_AG_lab_positive"] = pd.to_datetime(df_ED["COVID_first_PCR_or_AG_lab_positive"])
    df_ED["visit_start_date"] = pd.to_datetime(df_ED["visit_start_date"])
    df_ED["visit_end_date"] = pd.to_datetime(df_ED["visit_end_date"])
    
    df_ED['lab_minus_ED_visit_start_date'] = (df_ED['COVID_first_PCR_or_AG_lab_positive'] - df_ED['visit_start_date']).dt.days

    
    
    #create parameter for toggling COVID-19 related ED only visit and hospital admission definitions
    #when parameter =True: Per CDC definitions of a COVID-19 associated ED or hospital admission visit, ensure that a COVID-19 diagnosis and ED/hospital admission occurred in the 16 days after or 1 day prior to the PCR or AG positive test (index event).
    #when parameter =False: ED or hospital admission visits flagged based on the first instance of a positive COVID-19 PCR or AG lab result OR the first instance of a charted COVID-19 diagnosis when there is no positive lab result within specified timeframe of ED/hospital admission.

    covid_associated_ED_or_hosp_requires_lab_AND_diagnosis = True
    num_days_before_index = 1
    num_days_after_index = 16
    
    if covid_associated_ED_or_hosp_requires_lab_AND_diagnosis:
        df_ED['covid_pcr_or_ag_associated_ED_only_visit'] = np.where(df_ED['lab_minus_ED_visit_start_date'].between(-num_days_after_index, 
                                                                                                                    num_days_before_index, 
                                                                                                                    inclusive='both'), 1, 0)
        df_ED['COVID_lab_positive_and_diagnosed_ED_visit'] = np.where((df_ED['covid_pcr_or_ag_associated_ED_only_visit'] == 1) & 
                                                                      (df_ED['lab_minus_diagnosis_date'].between(-num_days_after_index,
                                                                                                                      num_days_before_index, 
                                                                                                                      inclusive='both')), 1, 0)
        df_ED = df_ED[df_ED['COVID_lab_positive_and_diagnosed_ED_visit'] == 1]
        df_ED.rename(columns={'visit_start_date': 'covid_ED_only_start_date'}, inplace=True)
        df_ED = df_ED[['person_id', 'covid_ED_only_start_date']]
        df_ED = df_ED.drop_duplicates()
            
    else:
        df_ED["COVID_first_poslab_or_diagnosis_date"] = pd.to_datetime(df_ED["COVID_first_poslab_or_diagnosis_date"]) 
        df_ED["visit_start_date"] = pd.to_datetime(df_ED["visit_start_date"])
        df_ED["visit_end_date"] = pd.to_datetime(df_ED["visit_end_date"])
        
        df_ED['earliest_index_minus_ED_start_date'] = (df_ED['COVID_first_poslab_or_diagnosis_date'] - df_ED['visit_start_date']).dt.days
        #first lab or diagnosis date based, ED only visit
        df_ED['covid_lab_or_dx_associated_ED_only_visit'] = np.where(df_ED['earliest_index_minus_ED_start_date'].between(-num_days_after_index,
                                                                                                                      num_days_before_index, 
                                                                                                                      inclusive='both'), 1, 0)
        df_ED = df_ED[df_ED['covid_lab_or_dx_associated_ED_only_visit'] == 1]
        df_ED.rename(columns={'visit_start_date': 'covid_ED_only_start_date'}, inplace=True)
        df_ED = df_ED[['person_id', 'covid_ED_only_start_date']]
        df_ED = df_ED.drop_duplicates()
   
    # use visit_occurrence table to find visits associated with hospitalization 
    ## Change for UVA Health Data: below code customized for visit_occurrence table
    hosp_concept_ids = list(concepts_df.loc[(concepts_df['concept_set_name'] == 'Hospitalization') & 
                            (concepts_df['is_most_recent_version'] == 't')]['concept_id'])
    df_hosp = df[(df['visit_start_date'].notna()) & (df['visit_concept_id'].isin(hosp_concept_ids))]
    
    df_hosp["COVID_first_PCR_or_AG_lab_positive"] = pd.to_datetime(df_hosp["COVID_first_PCR_or_AG_lab_positive"]) 
    df_hosp["visit_start_date"] = pd.to_datetime(df_hosp["visit_start_date"])
    df_hosp["visit_end_date"] = pd.to_datetime(df_hosp["visit_end_date"])
    
    df_hosp['lab_minus_hosp_start_date'] = (df_hosp['COVID_first_PCR_or_AG_lab_positive'] - df_hosp['visit_start_date']).dt.days
    
    
    if covid_associated_ED_or_hosp_requires_lab_AND_diagnosis:
        df_hosp['covid_pcr_or_ag_associated_hospitalization'] = np.where(df_hosp['lab_minus_hosp_start_date'].between(-num_days_after_index, 
                                                                                                                    num_days_before_index, 
                                                                                                                    inclusive='both'), 1, 0)
        df_hosp['COVID_lab_positive_and_diagnosed_hospitalization'] = np.where((df_hosp['covid_pcr_or_ag_associated_hospitalization'] == 1) & 
                                                                      (df_hosp['lab_minus_diagnosis_date'].between(-num_days_after_index,
                                                                                                                      num_days_before_index, 
                                                                                                                      inclusive='both')), 1, 0)
        df_hosp = df_hosp[df_hosp['COVID_lab_positive_and_diagnosed_hospitalization'] == 1]
        df_hosp.rename(columns={'visit_start_date': 'covid_hospitalization_start_date', 'visit_end_date':'covid_hospitalization_end_date'},
                       inplace=True)
        df_hosp = df_hosp[['person_id', 'covid_hospitalization_start_date', 'covid_hospitalization_end_date']]
        df_hosp = df_hosp.drop_duplicates()

    else:
        df_hosp["COVID_first_poslab_or_diagnosis_date"] = pd.to_datetime(df_hosp["COVID_first_poslab_or_diagnosis_date"]) 
        df_hosp["visit_start_date"] = pd.to_datetime(df_hosp["visit_start_date"])
        df_hosp["visit_end_date"] = pd.to_datetime(df_hosp["visit_end_date"])
        
        df_hosp['earliest_index_minus_hosp_start_date'] = (df_hosp['COVID_first_poslab_or_diagnosis_date'] - df_hosp['visit_start_date']).dt.days
        #first lab or diagnosis date based, ED only visit
        df_hosp['covid_lab_or_diagnosis_associated_hospitilization'] = np.where(df_hosp['earliest_index_minus_hosp_start_date'].between(-num_days_after_index,
                                                                                                                      num_days_before_index, 
                                                                                                                      inclusive='both'), 1, 0)
        df_hosp = df_hosp[df_hosp['covid_lab_or_diagnosis_associated_hospitilization'] == 1]
        df_hosp.rename(columns={'visit_start_date': 'covid_hospitalization_start_date', 'visit_end_date':'covid_hospitalization_end_date'},
                       inplace=True)
        df_hosp = df_hosp[['person_id', 'covid_hospitalization_start_date', 'covid_hospitalization_end_date']]
        df_hosp = df_hosp.drop_duplicates()
 
    #join ED and hosp dataframes
    df = df.merge(df_ED, on = 'person_id', how = 'outer')
    df = df.merge(df_hosp, on = 'person_id', how = 'outer')

    df = df.groupby('person_id').agg({'covid_ED_only_start_date':np.min, 'covid_hospitalization_start_date':np.min, 
                                 'covid_hospitalization_end_date':np.min}).reset_index()
    df.rename(columns={'covid_ED_only_start_date':'first_COVID_ED_only_start_date', 'covid_hospitalization_start_date':'first_COVID_hospitalization_start_date', 
                      'covid_hospitalization_end_date':'first_COVID_hospitalization_end_date'}, inplace = True)

    return df


# COVID_deaths

def COVID_deaths(death, COHORT, visit_occurrence, concept_set_members):
    """
    COVID_deaths:
        desc: finds death concepts
        ext: py
        inputs:
            - death
            - COHORT
            - visit_occurrence ## Change for UVA Health Data: visit_occurrence replaces micro_to_macrovisits table
            - concept_set_members
     

    Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for the confirmed (positive COVID PCR or AG lab test or U07.1 diagnosis charted) COVID positive patients in the UVA Health data.
    Creator/Owner/contact - Andrea Zhou
    Last Update - 3/28/23
    Description - This node filters the visits table for rows that have a discharge_to_concept_id that corresponds with the DECEASED or HOSPICE concept sets and combines these records with the patients in the deaths table. Death dates are taken from the deaths table and the visits table if the patient has a discharge_to_concept_id that corresponds with the DECEASED concept set. No date is retained for patients who were discharged to hospice. The node then drops any duplicates from this combined table, finds the earliest available death_date for each patient, and creates a flag for whether a patient has died.
    """
    
    persons = COHORT[['person_id', 'data_extraction_date']]
    
    concepts_df = concept_set_members[['concept_set_name', 'is_most_recent_version', 'concept_id']]
    concepts_df = concepts_df.loc[concepts_df['is_most_recent_version'] == 't']
    
    visits_df = visit_occurrence[['person_id','visit_end_date','discharge_to_concept_id']]
    visits_df.rename(columns={'visit_end_date': 'death_date'}, inplace=True)
    
    death_df = death[['person_id', 'death_date']]
    death_df = death_df.drop_duplicates()

    #create lists of concept ids to look for in the discharge_to_concept_id column of the visits_df
    death_from_visits_ids = list(concepts_df.loc[concepts_df['concept_set_name'] == 'DECEASED']['concept_id'])
    hospice_from_visits_ids = list(concepts_df.loc[concepts_df['concept_set_name'] == 'HOSPICE']['concept_id'])
    

    #filter visits table to patient and date rows that have DECEASED that matches list of concept_ids
    death_from_visits_df = visits_df[visits_df['discharge_to_concept_id'].isin(death_from_visits_ids)]
    death_from_visits_df = death_from_visits_df.drop(['discharge_to_concept_id'], axis = 1)
    death_from_visits_df = death_from_visits_df.drop_duplicates()
    
    #filter visits table to patient rows that have DECEASED that matches list of concept_ids
    hospice_from_visits_df = visits_df.drop(['death_date'], axis = 1)
    hospice_from_visits_df = hospice_from_visits_df[hospice_from_visits_df['discharge_to_concept_id'].isin(hospice_from_visits_ids)]
    hospice_from_visits_df = hospice_from_visits_df.drop(['discharge_to_concept_id'], axis = 1)
    hospice_from_visits_df = hospice_from_visits_df.drop_duplicates()

    ###combine relevant visits sourced deaths from deaths table deaths###

    #joining in deaths from visits table to deaths table
    #join in patients, without any date, for HOSPICE
    #inner join to persons to only keep info related to desired cohort
    df = death_df.merge(death_from_visits_df, on=['person_id', 'death_date'], how='outer')
    df = df.merge(hospice_from_visits_df, on='person_id', how='outer')
    df = df.merge(persons, on='person_id', how='inner')
        
    #collapse to unique person and find earliest date the patient expired or was discharge to hospice 
    df = df.groupby('person_id').agg({'death_date':'min', 'data_extraction_date':'max'}).reset_index()
    df.rename(columns={'death_date': 'date'}, inplace=True)
    
    df['COVID_patient_death'] = 1
    
    return df


# cohort_all_facts_table

def cohort_all_facts_table(conditions_of_interest, measurements_of_interest, visits_of_interest, procedures_of_interest, observations_of_interest, drugs_of_interest, COVID_deaths, COHORT, devices_of_interest, visit_occurrence): 
    """
    cohort_all_facts_table:
        desc: creates date level table with one row per patient per date
        ext: py
        inputs:
            - conditions_of_interest
            - measurements_of_interest
            - visits_of_interest
            - procedures_of_interest
            - observations_of_interest
            - drugs_of_interest
            - COVID_deaths
            - COHORT
            - devices_of_interest
            - visit_occurrence ## Change for UVA Health Data: visit_occurrence replaces micro_to_macrovisits table
        ## Change for UVA Health Data: vaccines_of_interest table not inputted
     

    Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for the confirmed (positive COVID PCR or AG lab test or U07.1 diagnosis charted) COVID positive patients in the UVA Health data.
    Creator/Owner/contact - Andrea Zhou
    Last Update - 3/28/23
    Description - All facts collected in the previous steps are combined in this cohort_all_facts_table on the basis of unique days for each patient and logic is applied to see if the instance of the coded fact appeared in the EHR prior to or after the patient's first COVID-19 positive PCR or AG lab test.  Indicators are created for the presence or absence of events, medications, conditions, measurements, device exposures, observations, procedures, and outcomes, either occurring before COVID index date, during the patient’s hospitalization, or in the period after COVID index date.  It also creates an indicator for whether the date where a fact was noted occurred during any hospitalization, not just the COVID associated ones found in the visits of interest node.  A flag for the visit in which the patient is noted to have their first covid reinfection is also in this node. The default time range is 60 days after index date, but the number of days can be specified by parameter input based on researcher interests. This table is useful if the analyst needs to use actual dates of events as it provides more detail than the final patient-level table.  Use the max and min functions to find the first and last occurrences of any events.

    """
    
    persons_df = COHORT[['person_id', 'COVID_first_PCR_or_AG_lab_positive', 'COVID_first_diagnosis_date', 'COVID_first_poslab_or_diagnosis_date']]

    visit_occurrence_df = visit_occurrence
    procedures_df = procedures_of_interest
    devices_df = devices_of_interest
    observations_df = observations_of_interest
    conditions_df = conditions_of_interest
    drugs_df = drugs_of_interest
    measurements_df = measurements_of_interest
    visits_df = visits_of_interest
    
    deaths_df = COVID_deaths[COVID_deaths['date'].notna()]
    
    deaths_df["date"] = pd.to_datetime(deaths_df["date"]).dt.date 
    deaths_df["data_extraction_date"] = pd.to_datetime(deaths_df["data_extraction_date"]).dt.date 
    
    deaths_df = deaths_df[deaths_df['date'] >= datetime.date(2018, 1, 1)]
    
    deaths_df = deaths_df[deaths_df['date'] < (deaths_df['data_extraction_date'] + datetime.timedelta(days=365*2))]
    deaths_df = deaths_df.drop(['data_extraction_date'], axis = 1)
    
    df = visit_occurrence_df[['person_id','visit_start_date']]
    df.rename(columns={'visit_start_date': 'date'}, inplace=True)
    df = df.merge(procedures_df, on = ['person_id','date'], how='outer')
    df = df.merge(devices_df, on = ['person_id','date'], how='outer')
    df = df.merge(observations_df, on = ['person_id','date'], how='outer')
    df = df.merge(conditions_df, on = ['person_id','date'], how='outer')
    df = df.merge(drugs_df, on = ['person_id','date'], how='outer')
    df = df.merge(measurements_df, on = ['person_id','date'], how='outer')
    df = df.merge(deaths_df, on = ['person_id','date'], how='outer')
    
    df[df.columns.difference(['BMI_rounded'])] = df[df.columns.difference(['BMI_rounded'])].fillna(0)
   
    #add F.max of all indicator columns to collapse all cross-domain flags to unique person and visit rows
    df = df.groupby(['person_id', 'date']).max().reset_index()
   
    #join persons
    df = persons_df.merge(df, on = 'person_id', how = 'left')
    df = visits_df.merge(df, on = 'person_id', how = 'outer')

    #create reinfection indicator, minimum 60 day window from index date to subsequent positive test
    reinfection_wait_time = 60
    
    reinfection_df = df
    
    reinfection_df["date"] = pd.to_datetime(reinfection_df["date"]).dt.date 
    
    reinfection_df['is_reinfection'] = np.where((reinfection_df['PCR_AG_Pos'] == 1) & 
                                                (((reinfection_df['date'] - reinfection_df['COVID_first_poslab_or_diagnosis_date']).dt.days) > 
                                                 reinfection_wait_time), 1, 0)
    reinfection_df = reinfection_df[reinfection_df['is_reinfection'] == 1]
    reinfection_df = reinfection_df.groupby('person_id').agg({'date':'min', 'is_reinfection':'max'}).reset_index()
    reinfection_df.rename(columns={'is_reinfection': 'is_first_reinfection'}, inplace=True)
    
    df = df.merge(reinfection_df, on=['person_id','date'], how='left')

    #create new death within fixed window post COVID flag to be used for severity calculation, same window as looking for reinfection 
    
    df['death_within_specified_window_post_covid'] = np.where((df['COVID_patient_death'] == 1) & 
                                                (((df['date'] - df['COVID_first_poslab_or_diagnosis_date']).dt.days) > 0) & 
                                                (((df['date'] - df['COVID_first_poslab_or_diagnosis_date']).dt.days) < reinfection_wait_time), 
                                                              1, 0)

    
    #defaulted to find the lesser date value of the first lab positive result date and the first diagnosis date, could be adjusted to only "COVID_first_diagnosis_date" or only "COVID_first_PCR_or_AG_lab_positive" based on desired index event definition
    df['pre_COVID'] = np.where(((df['COVID_first_poslab_or_diagnosis_date'] - df['date']).dt.days) >= 0, 1, 0) 
    df['post_COVID'] = np.where(((df['COVID_first_poslab_or_diagnosis_date'] - df['date']).dt.days) < 0, 1, 0) 
    
    #dependent on the definition chosen in the visits of interest node, no changes necessary here
    df['first_COVID_hospitalization_end_date'] = pd.to_datetime(df['first_COVID_hospitalization_end_date'])
    df['first_COVID_ED_only_start_date'] = pd.to_datetime(df['first_COVID_ED_only_start_date'])
    df['date'] = pd.to_datetime(df['date'])
    
    df['during_first_COVID_hospitalization'] = np.where((((df['first_COVID_hospitalization_end_date'] - df['date']).dt.days) >= 0) & 
                                                (((df['first_COVID_hospitalization_start_date'] - df['date']).dt.days) <= 0), 1, 0)
    df['during_first_COVID_ED_visit'] = np.where(((df['first_COVID_ED_only_start_date'] - df['date']).dt.days) == 0, 1, 0)

    #drop dates for all facts table once indicators are created for 'during_first_COVID_hospitalization'
    ## Change for UVA Health Data: 'visit_start_date' and 'visit_end_date' are removed from list
    df = df.drop(['first_COVID_hospitalization_start_date', 'first_COVID_hospitalization_end_date','first_COVID_ED_only_start_date'], axis=1)
    
    #create and join in flag that indicates whether the visit hospitalization was during the visit occurrence (1) or not (0)
    #any conditions, observations, procedures, devices, drugs, measurements, and/or death flagged 
    #with a (1) on that particular visit date would then be considered to have happened during the visit occurrence
    
    ## Change for UVA Health Data: below code customized for visit_occurrence table   
    visit_occurrence_df = visit_occurrence_df[['person_id', 'visit_start_date', 'visit_end_date']]
    visit_occurrence_df = visit_occurrence_df[visit_occurrence_df['visit_start_date'].notna() & visit_occurrence_df['visit_end_date'].notna()]
    visit_occurrence_df = visit_occurrence_df.drop_duplicates()
    
    df_hosp = df[['person_id', 'date']]
    df_hosp = df_hosp.merge(visit_occurrence_df, on='person_id', how= 'outer')
    
    df_hosp['visit_end_date'] = pd.to_datetime(df_hosp['visit_end_date'])
    df_hosp['visit_start_date'] = pd.to_datetime(df_hosp['visit_start_date'])
    df_hosp['date'] = pd.to_datetime(df_hosp['date'])
    
    df_hosp['during_visit_hospitalization'] = np.where((((df_hosp['visit_end_date'] - df_hosp['date']).dt.days) >= 0) & 
                                                           (((df_hosp['visit_start_date'] - df_hosp['date']).dt.days) <= 0), 
                                                           1, 0)
    
    df_hosp = df_hosp.drop(['visit_start_date', 'visit_end_date'], axis = 1)
    df_hosp = df_hosp[df_hosp['during_visit_hospitalization'] == 1]
    df_hosp = df_hosp.drop_duplicates()
    
    df = df.merge(df_hosp, on=['person_id','date'], how="left")
    
    #final fill of null non-continuous variables with 0
    df[df.columns.difference(['BMI_rounded'])] = df[df.columns.difference(['BMI_rounded'])].fillna(0)

    return df


# COVID_Patient_Summary_Table_LDS

def COVID_Patient_Summary_Table_LDS(cohort_all_facts_table, COHORT, visits_of_interest, COVID_deaths, customize_concept_sets):
    """
    COVID_Patient_Summary_Table_LDS:
        desc: creates summary table with one row per patient
        ext: py
        inputs:
            - cohort_all_facts_table
            - COHORT
            - visits_of_interest
            - COVID_deaths
            - customize_concept_sets
        ## Change for UVA Health Data: Sdoh_variables_all_patients table not inputted
 

    Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for the confirmed (positive COVID PCR or AG lab test or U07.1 diagnosis charted) COVID positive patients in the UVA Health data.
    Creator/Owner/contact - Andrea Zhou
    Last Update - 3/28/23
    Description - The final step is to aggregate information to create a data frame that contains a single row of data for each patient in the cohort.  This node aggregates all information from the cohort_all_facts_table and summarizes each patient's facts in a single row.  The patient’s hospitalization length of stay is calculated in this node.  For patients with ED visits and/or hospitalizations concurrent with their positive COVID-19 index date, indicators are created in this node. This transformation then joins the before or day of COVID, during hospitalization, and post COVID indicator data frames on the basis of unique patients.

    """

    visits_df = visits_of_interest
    deaths_df = COVID_deaths[['person_id','COVID_patient_death']]
    all_facts = cohort_all_facts_table
    fusion_sheet = customize_concept_sets

    pre_columns = list(fusion_sheet[fusion_sheet.pre_during_post.str.contains('pre')]['indicator_prefix'].unique())
    pre_columns.extend(['person_id', 'BMI_rounded', 'Antibody_Pos', 'Antibody_Neg'])
    
    during_columns = list(fusion_sheet[fusion_sheet.pre_during_post.str.contains('during')]['indicator_prefix'].unique())
    during_columns.extend(['person_id', 'COVID_patient_death'])
    
    post_columns = list(fusion_sheet[fusion_sheet.pre_during_post.str.contains('post')]['indicator_prefix'].unique())
    post_columns.extend(['person_id', 'BMI_rounded', 'PCR_AG_Pos', 'PCR_AG_Neg', 'Antibody_Pos', 'Antibody_Neg', 'is_first_reinfection'])
    
    df_pre_COVID = all_facts[all_facts.pre_COVID==1]
    df_pre_COVID = df_pre_COVID[df_pre_COVID.columns.intersection(list(set(pre_columns) & set(all_facts.columns)))]
    
    df_during_COVID_hospitalization = all_facts[all_facts.during_first_COVID_hospitalization==1]
    df_during_COVID_hospitalization = df_during_COVID_hospitalization[df_during_COVID_hospitalization.columns.intersection(list(set(during_columns) 
                                                                                                                                & set(all_facts.columns)))]
    
 
    df_post_COVID = all_facts[all_facts.post_COVID==1]
    df_post_COVID = df_post_COVID[df_post_COVID.columns.intersection(list(set(post_columns) & set(all_facts.columns)))]
    
        
    pre_other_cols = [col for col in  df_pre_COVID.columns if col not in ['person_id', 'BMI_rounded']]    
    df_pre_COVID1 = df_pre_COVID.groupby('person_id').agg(BMI_max_observed_or_calculated_before_or_day_of_covid=('BMI_rounded', 'max')) 
    df_pre_COVID2 = df_pre_COVID.groupby('person_id').agg({col: 'max' for col in pre_other_cols})
    df_pre_COVID2.rename(columns=lambda col: str(col + '_before_or_day_of_covid_indicator'), inplace=True)
    df_pre_COVID = df_pre_COVID1.merge(df_pre_COVID2, on = 'person_id').reset_index()

    
    during_other_cols = [col for col in  df_during_COVID_hospitalization.columns if col not in ['person_id']]
    df_during_COVID_hospitalization = df_during_COVID_hospitalization.groupby('person_id').agg({col: 'max' for col in during_other_cols})
    df_during_COVID_hospitalization.rename(columns=lambda col: str(col + '_during_covid_hospitalization_indicator'), inplace=True)
    df_during_COVID_hospitalization = df_during_COVID_hospitalization.reset_index()

    
    post_other_cols = [col for col in  df_post_COVID.columns if col not in ['person_id', 'BMI_rounded', 'is_first_reinfection']]    
    df_post_COVID1 = df_post_COVID.groupby('person_id').agg(BMI_max_observed_or_calculated_post_covid=('BMI_rounded', 'max'),
                                                           had_at_least_one_reinfection_post_covid_indicator = ('is_first_reinfection', 'max')) 
    df_post_COVID2 = df_post_COVID.groupby('person_id').agg({col: 'max' for col in post_other_cols})
    df_post_COVID2.rename(columns=lambda col: str(col + '_post_covid_indicator'), inplace=True)
    df_post_COVID = df_post_COVID1.merge(df_post_COVID2, on = 'person_id').reset_index()
    
    #join above three tables on patient ID 
    df = pd.merge(df_pre_COVID,df_during_COVID_hospitalization, on='person_id', how='outer')
    df = pd.merge(df, df_post_COVID, on='person_id', how='outer')
    
    df = pd.merge(df, visits_df,on='person_id', how='outer')

    #already dependent on decision made in visits of interest node, no changes necessary here
    df['COVID_hospitalization_length_of_stay'] = (df["first_COVID_hospitalization_end_date"] - df["first_COVID_hospitalization_start_date"]).dt.days
    
    df['COVID_associated_ED_only_visit_indicator'] = np.where(df.first_COVID_ED_only_start_date.notnull(), 1, 0)
    df['COVID_associated_hospitalization_indicator'] = np.where(df.first_COVID_hospitalization_start_date.notnull(), 1, 0)

    #join back in generic death flag for any patient with or without a date
    df = df.merge(deaths_df,on='person_id',how='left')
    df.rename(columns={'COVID_patient_death':'COVID_patient_death_indicator'},inplace=True)
    
    #join back in death within fixed window post covid for patients with a date to use in severity of index infection
    df = df.merge(all_facts[['person_id', 'death_within_specified_window_post_covid']].loc[all_facts['death_within_specified_window_post_covid'] == 1], 
                 on = 'person_id', how = 'left')
    
    #join in demographics and manifest data from cohort node
    df = COHORT.merge(df, on='person_id',how='left')
    
    df[df.columns.difference(['BMI_max_observed_or_calculated_before_or_day_of_covid','BMI_max_observed_or_calculated_post_covid', 'postal_code', 
                              'age_at_covid'])] = df[df.columns.difference(['BMI_max_observed_or_calculated_before_or_day_of_covid',
                                                                            'BMI_max_observed_or_calculated_post_covid', 'postal_code', 'age_at_covid'])].fillna(0)
    
    conditions_COVID = [
          ((pd.isnull(df.COVID_first_PCR_or_AG_lab_positive)) & (pd.isnull(df.COVID_first_diagnosis_date))),
          (df.death_within_specified_window_post_covid == 1) ,
          ((df.LL_ECMO_during_covid_hospitalization_indicator == 1)| (df.LL_IMV_during_covid_hospitalization_indicator == 1)),
          (pd.notnull(df.first_COVID_hospitalization_start_date)),
          (pd.notnull(df.first_COVID_ED_only_start_date)),
          
        ]
    choices_COVID = ["No_COVID_index","Death_within_n_days_after_COVID_index",
                            "Severe_ECMO_IMV_in_Hosp_around_COVID_index","Moderate_Hosp_around_COVID_index",
                         "Mild_ED_around_COVID_index"]

    df["Severity_Type"] = np.select(conditions_COVID, choices_COVID, default="Mild_No_ED_or_Hosp_around_COVID_index")

    return df