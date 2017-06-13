/* query to compare the ecd data generated with my understanding of the SEED database structure */
create table test_seed_data_source as (
	  with full_data as 
	  (
		  select 
		  a.registrant_id,
		  b.id,
		  a.data,
		  b.details,
		  a.data->>'edd' as edd,
		  b.details->>'last_edd' as last_edd,
		  replace(a.data->>'msisdn_device','+','') as msisdn_device,
		  replace(a.data->>'msisdn_registrant','+','') as msisdn_registrant,
		  c."MSISDN" as msisdn_ecd_participants,
		  a.data ->>'faccode' as facility_code,
		  a.data->>'language' as language,
		  a.data->>'mom_dob' as mom_dob,
		  a.data->>'id_type' as id_type,
		  a.data->>'sa_id_no' as sa_id_no,
		  a.data->>'passport_no' as passport_number,
		  a.data->>'passport_origin' as passport_origin,
		  d.identity,
		  d.lang,
		  d.active,
		  d.messageset_id
		  from seed_hub_schema.registrations_registration as a
		  join seed_identity_schema.identities_identity as b
		  on a.registrant_id::uuid = b.id::uuid
		  left join  ecd_full_sample_data_set_deduped as c 
		  on replace(a.data->>'msisdn_registrant','+','') = c."MSISDN"
		  join seed_stage_based_messaging_schema.subscriptions_subscription as d
		  on a.registrant_id::uuid = d.identity::uuid
		  order by msisdn_registrant,edd asc /*primary key */
		  )
		  select 
		  *
		  from full_data as a
		  left join clinic_facilities_with_gps as b
		  on a.facility_code::integer = b.facilitycode::integer
		  order by edd,msisdn_registrant
		  limit 100000
		  )
		;
