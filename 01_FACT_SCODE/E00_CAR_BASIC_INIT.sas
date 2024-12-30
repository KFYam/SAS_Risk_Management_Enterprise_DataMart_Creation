/* ************************************************************************************************ */
/* To import the source file in SAS dataset in characters format for avoiding information loss.		*/
/* The SAS dataset in characters format is saved in libname "STG", we will have regular				*/
/* housekeeping on every 2 years in this libname "STG".												*/
/* ************************************************************************************************ */

%macro gen_initial_CAR_SRC_FIRB();
%if &flag_initial. = YES %then %do;
	%import_InputMaster(sheet=MetaInfo_CAR_T_CDR,	out=raw_car.meta_CAR_T_CDR_initial);

%end;
%mend;

%macro gen_initial_entity_SRC_FIRB();
%if &flag_initial. = YES %then %do;
	%import_InputMaster(sheet=MetaInfo_CAR_ENTITY,	out=raw_car.meta_CAR_ENTITY_initial);

	%gen_SRC_IN_CHAR(orig_src=%str(&dir_car./SRC_FIRB/FIRB entity 200912 to 201012.csv), orig_delim=",", sas_tblname=stg_car.CAR_ENTITY_200912_201012, realtime_meta=work.rtmeta_CAR_ENTITY_200912_201012, encode=); 
	%update_META2SRC(metatbl=stg_car.rtmeta_CAR_ENTITY_201312, srctbl=stg_car.CAR_ENTITY_201312, tartbl=raw_car.CAR_ENTITY_201312);
%end;
%mend;
